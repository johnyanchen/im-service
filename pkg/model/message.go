package model

import (
	"context"
	"time"
)

type Message struct {
	ID             int64
	ConversationID int64
	FromID         int64
	FromUsername   string
	Content        string
	CreatedAt      time.Time
}

func (db *DB) CreateMessage(ctx context.Context, convID, fromID int64, content string) (*Message, error) {
	msg := &Message{}
	err := db.Pool.QueryRow(ctx,
		"INSERT INTO messages(conversation_id, from_id, content) VALUES($1, $2, $3) RETURNING id, conversation_id, from_id, content, created_at",
		convID, fromID, content).Scan(&msg.ID, &msg.ConversationID, &msg.FromID, &msg.Content, &msg.CreatedAt)
	return msg, err
}

func (db *DB) GetMessagesBefore(ctx context.Context, convID, beforeID int64, limit int, minVisibleID int64) ([]*Message, error) {
	if limit <= 0 {
		limit = 30
	}
	// minVisibleID：调用者在该会话的可见下界（删好友再加回后只看重新聊起之后的消息）。
	// 统一加 m.id > minVisibleID 过滤；默认 0 表示可见全部。
	var query string
	var args []interface{}
	if beforeID > 0 {
		query = `SELECT m.id, m.conversation_id, m.from_id, u.username, m.content, m.created_at
			 FROM messages m JOIN users u ON u.id = m.from_id
			 WHERE m.conversation_id=$1 AND m.id < $2 AND m.id > $4 ORDER BY m.id DESC LIMIT $3`
		args = []interface{}{convID, beforeID, limit, minVisibleID}
	} else {
		query = `SELECT m.id, m.conversation_id, m.from_id, u.username, m.content, m.created_at
			 FROM messages m JOIN users u ON u.id = m.from_id
			 WHERE m.conversation_id=$1 AND m.id > $3 ORDER BY m.id DESC LIMIT $2`
		args = []interface{}{convID, limit, minVisibleID}
	}
	rows, err := db.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var msgs []*Message
	for rows.Next() {
		m := &Message{}
		if err := rows.Scan(&m.ID, &m.ConversationID, &m.FromID, &m.FromUsername, &m.Content, &m.CreatedAt); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	// 反转为正序（id ASC）
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}
	return msgs, nil
}
