package main

import (
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
)

// connSeq 为每条连接分配一个进程内唯一的自增 ID，
// 用于在清理时区分"同一 userID 的新旧连接"。
var connSeq atomic.Int64

type Conn struct {
	id int64
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
	old := h.conns[userID]
	h.conns[userID] = conn
	h.mu.Unlock()

	// 在锁外踢掉旧连接，避免在 Hub 全局写锁内做网络 IO。
	if old != nil && old != conn {
		old.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(4001, "kicked"))
		old.Close()
	}
}

// Remove 仅在当前存的正是 conn 本身时才删除，
// 避免旧连接退出时误删已经换绑的新连接。
func (h *Hub) Remove(userID int64, conn *Conn) {
	h.mu.Lock()
	if h.conns[userID] == conn {
		delete(h.conns, userID)
	}
	h.mu.Unlock()
}

func (h *Hub) Get(userID int64) *Conn {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.conns[userID]
}
