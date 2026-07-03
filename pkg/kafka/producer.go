package kafka

import (
	"encoding/json"

	"github.com/IBM/sarama"
)

const TopicMessageFanout = "message_fanout"

// FanoutEvent 的事件类型。空字符串视为 new_message（向后兼容旧消息）。
const (
	EventNewMessage  = "new_message"
	EventConvCreated = "conv_created"
	EventFriend      = "friend_event"
)

// 好友事件的动作，写入 friend_event 推送的 action 字段。
const (
	FriendActionRequest  = "request"  // 收到好友申请
	FriendActionAccepted = "accepted" // 申请被同意
	FriendActionRejected = "rejected" // 申请被拒绝
)

type FanoutEvent struct {
	EventType        string `json:"event_type"`
	MessageID        int64  `json:"message_id"`
	ConversationID   int64  `json:"conversation_id"`
	ConversationType string `json:"conversation_type"`
	ConversationName string `json:"conversation_name"`
	FromID           int64  `json:"from_id"`
	FromUsername     string `json:"from_username"`
	Content          string `json:"content"`
	CreatedAt        int64  `json:"created_at"`
	// Members 仅 conv_created 使用：新会话的成员，避免依赖 Redis 成员缓存的时序。
	Members []int64 `json:"members,omitempty"`

	// 以下字段仅 friend_event 使用。
	// TargetID 是被推送的单个用户（好友事件不做会话成员扩散）。
	// FriendAction 取 FriendAction* 常量；RequestID 便于客户端定位这条申请。
	TargetID     int64  `json:"target_id,omitempty"`
	FriendAction string `json:"friend_action,omitempty"`
	RequestID    int64  `json:"request_id,omitempty"`
}

type Producer struct {
	producer sarama.SyncProducer
}

func NewProducer(brokers []string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}
	return &Producer{producer: p}, nil
}

func (p *Producer) PublishFanout(event *FanoutEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, _, err = p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: TopicMessageFanout,
		Value: sarama.ByteEncoder(data),
	})
	return err
}

func (p *Producer) Close() error {
	return p.producer.Close()
}
