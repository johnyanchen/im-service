package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

const (
	pongWait    = 60 * time.Second
	pingPeriod  = 30 * time.Second
	routeTTL    = 5 * time.Minute
	renewPeriod = 2 * time.Minute
)

func (s *GatewayServer) handleWS(conn *Conn, userID int64, myRoute string) {
	ticker := time.NewTicker(renewPeriod)
	pingTicker := time.NewTicker(pingPeriod)
	done := make(chan struct{})

	defer func() {
		ticker.Stop()
		pingTicker.Stop()
		close(done) // 通知续期/心跳 goroutine 退出，避免泄漏
		s.hub.Remove(userID, conn)
		s.redis.DelRouteIf(context.Background(), userID, myRoute)
		conn.Close()
	}()

	conn.ws.SetReadDeadline(time.Now().Add(pongWait))
	conn.ws.SetPongHandler(func(string) error {
		conn.ws.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// 路由续期 + 心跳 Ping
	go func() {
		for {
			select {
			case <-done: // 唯一可靠的退出路径
				return
			case <-ticker.C:
				s.redis.SetRoute(context.Background(), userID, myRoute)
			case <-pingTicker.C:
				conn.mu.Lock()
				conn.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
				err := conn.ws.WriteMessage(websocket.PingMessage, nil)
				conn.ws.SetWriteDeadline(time.Time{})
				conn.mu.Unlock()
				if err != nil {
					return
				}
			}
		}
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			return
		}
	}
}

func (s *GatewayServer) writeJSON(conn *Conn, v interface{}) {
	data, _ := json.Marshal(v)
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		log.Printf("ws write error: %v", err)
	}
}
