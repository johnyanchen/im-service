package main

import (
	"sync"

	"github.com/gorilla/websocket"
)

type Conn struct {
	ws *websocket.Conn
	mu sync.Mutex
}

func (c *Conn) WriteMessage(msgType int, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ws.WriteMessage(msgType, data)
}

func (c *Conn) Close() error {
	return c.ws.Close()
}

func (c *Conn) ReadMessage() (int, []byte, error) {
	return c.ws.ReadMessage()
}

type Hub struct {
	mu    sync.RWMutex
	conns map[int64]*Conn
}

func NewHub() *Hub {
	return &Hub{conns: make(map[int64]*Conn)}
}

func (h *Hub) Add(userID int64, conn *Conn) {
	h.mu.Lock()
	if old, ok := h.conns[userID]; ok && old != conn {
		old.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(4001, "kicked"))
		old.Close()
	}
	h.conns[userID] = conn
	h.mu.Unlock()
}

func (h *Hub) Remove(userID int64) {
	h.mu.Lock()
	delete(h.conns, userID)
	h.mu.Unlock()
}

func (h *Hub) Get(userID int64) *Conn {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.conns[userID]
}
