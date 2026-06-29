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

func (s *GatewayServer) handleWS(conn *Conn, userID int64) {
	ticker := time.NewTicker(renewPeriod)
	pingTicker := time.NewTicker(pingPeriod)

	defer func() {
		ticker.Stop()
		pingTicker.Stop()
		s.hub.Remove(userID)
		s.redis.DelRoute(context.Background(), userID)
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
			case _, ok := <-ticker.C:
				if !ok {
					return
				}
				s.redis.SetRoute(context.Background(), userID, s.cfg.GatewayGRPC)
			case _, ok := <-pingTicker.C:
				if !ok {
					return
				}
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
