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

func (db *DB) GetMessagesSince(ctx context.Context, convID, sinceID int64, limit int) ([]*Message, error) {
	rows, err := db.Pool.Query(ctx,
		`SELECT m.id, m.conversation_id, m.from_id, u.username, m.content, m.created_at
		 FROM messages m JOIN users u ON u.id = m.from_id
		 WHERE m.conversation_id=$1 AND m.id > $2 ORDER BY m.id ASC LIMIT $3`,
		convID, sinceID, limit)
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
	return msgs, nil
}
