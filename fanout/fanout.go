package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"im-service/pkg/kafka"
	"im-service/pkg/model"
	"im-service/pkg/redisstore"
	pb "im-service/proto"
)

type FanoutProcessor struct {
	db       *model.DB
	redis    *redisstore.Store
	gateways map[string]pb.GatewayServiceClient
	mu       sync.RWMutex
}

func NewFanoutProcessor(db *model.DB, redis *redisstore.Store) *FanoutProcessor {
	return &FanoutProcessor{
		db:       db,
		redis:    redis,
		gateways: make(map[string]pb.GatewayServiceClient),
	}
}

func (f *FanoutProcessor) Handle(ctx context.Context, event *kafka.FanoutEvent) error {
	if event.EventType == kafka.EventConvCreated {
		return f.handleConvCreated(ctx, event)
	}

	members, err := f.redis.GetMembers(ctx, event.ConversationID)
	if err == redis.Nil {
		members, err = f.db.GetConversationMembers(ctx, event.ConversationID)
		if err != nil {
			return err
		}
		_ = f.redis.SetMembers(ctx, event.ConversationID, members)
	} else if err != nil {
		return err
	}

	sem := make(chan struct{}, 50)
	var wg sync.WaitGroup
	for _, uid := range members {
		wg.Add(1)
		sem <- struct{}{}
		go func(userID int64) {
			defer func() { <-sem; wg.Done() }()
			isSender := userID == event.FromID

			// 单聊时 conv_name 对每个人不同：存的是对方的名字
			convName := event.ConversationName
			if event.ConversationType == "dm" {
				if isSender {
					convName = event.ConversationName // 发送者看到的是对方名字（Logic已算好）
				} else {
					convName = event.FromUsername // 接收者看到的是发送者名字
				}
			}

			err := f.db.UpsertUserConversation(ctx, userID, event.ConversationID, event.MessageID, isSender, event.Content, event.FromUsername, event.ConversationType, convName)
			if err != nil {
				log.Printf("fanout: upsert uc error uid=%d: %v", userID, err)
			}
			route, err := f.redis.GetRoute(ctx, userID)
			if err != nil {
				return
			}
			client := f.getGatewayClient(redisstore.GatewayAddrOf(route))
			if client == nil {
				return
			}
			pushPayload, _ := json.Marshal(map[string]interface{}{
				"type":              "new_message",
				"message_id":        event.MessageID,
				"conversation_id":   event.ConversationID,
				"conversation_type": event.ConversationType,
				"conversation_name": convName,
				"from_id":           event.FromID,
				"from_username":     event.FromUsername,
				"content":           event.Content,
				"created_at":        event.CreatedAt,
			})
			_, err = client.Push(ctx, &pb.PushRequest{UserId: userID, Payload: pushPayload})
			if err != nil {
				log.Printf("fanout: push to %d via %s failed: %v", userID, route, err)
			}
		}(uid)
	}
	wg.Wait()
	return nil
}

// handleConvCreated 把"新会话"事件实时推给所有在线成员，
// 让会话立即出现在他们的会话列表里，无需客户端再次 sync。
// 会话视图行已由 Logic 在建会话时 seed，这里只负责推送。
func (f *FanoutProcessor) handleConvCreated(ctx context.Context, event *kafka.FanoutEvent) error {
	members := event.Members
	if len(members) == 0 {
		var err error
		members, err = f.db.GetConversationMembers(ctx, event.ConversationID)
		if err != nil {
			return err
		}
	}
	// 刷新成员缓存，保证随后的消息 fanout 能命中
	_ = f.redis.SetMembers(ctx, event.ConversationID, members)

	sem := make(chan struct{}, 50)
	var wg sync.WaitGroup
	for _, uid := range members {
		wg.Add(1)
		sem <- struct{}{}
		go func(userID int64) {
			defer func() { <-sem; wg.Done() }()

			// 单聊：每个成员看到的会话名是"对方"。Logic 已把发起者侧算成对端名，
			// 这里给非发起者推发起者的名字。群聊则统一用群名。
			convName := event.ConversationName
			if event.ConversationType == "dm" && userID != event.FromID {
				convName = event.FromUsername
			}

			route, err := f.redis.GetRoute(ctx, userID)
			if err != nil {
				return // 不在线，靠下次 sync 兜底
			}
			client := f.getGatewayClient(redisstore.GatewayAddrOf(route))
			if client == nil {
				return
			}
			payload, _ := json.Marshal(map[string]interface{}{
				"type":              "conv_created",
				"conversation_id":   event.ConversationID,
				"conversation_type": event.ConversationType,
				"conversation_name": convName,
				"created_at":        event.CreatedAt,
			})
			if _, err := client.Push(ctx, &pb.PushRequest{UserId: userID, Payload: payload}); err != nil {
				log.Printf("fanout: push conv_created to %d via %s failed: %v", userID, route, err)
			}
		}(uid)
	}
	wg.Wait()
	return nil
}

func (f *FanoutProcessor) getGatewayClient(addr string) pb.GatewayServiceClient {	f.mu.RLock()
	c, ok := f.gateways[addr]
	f.mu.RUnlock()
	if ok {
		return c
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.gateways[addr]; ok {
		return c
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("fanout: dial gateway %s: %v", addr, err)
		return nil
	}
	client := pb.NewGatewayServiceClient(conn)
	f.gateways[addr] = client
	return client
}
