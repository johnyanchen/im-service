package main

import (
	"sync"

	"github.com/gorilla/websocket"
)

type Hub struct {
	mu    sync.RWMutex
	conns map[int64]*websocket.Conn
}

func NewHub() *Hub {
	return &Hub{conns: make(map[int64]*websocket.Conn)}
}

func (h *Hub) Add(userID int64, conn *websocket.Conn) {
	h.mu.Lock()
	h.conns[userID] = conn
	h.mu.Unlock()
}

func (h *Hub) Remove(userID int64) {
	h.mu.Lock()
	delete(h.conns, userID)
	h.mu.Unlock()
}

func (h *Hub) Get(userID int64) *websocket.Conn {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.conns[userID]
}
