package model

import (
	"context"
	"time"
)

type UserConversation struct {
	UserID         int64
	ConversationID int64
	LastMsgID      int64
	UnreadCount    int32
	UpdatedAt      time.Time
}

func (db *DB) UpsertUserConversation(ctx context.Context, userID, convID, msgID int64, isSender bool) error {
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO user_conversations(user_id, conversation_id, last_msg_id, unread_count, updated_at)
		VALUES($1, $2, $3, CASE WHEN $4 THEN 0 ELSE 1 END, NOW())
		ON CONFLICT(user_id, conversation_id) DO UPDATE SET
			last_msg_id = $3,
			unread_count = CASE WHEN $4 THEN 0 ELSE user_conversations.unread_count + 1 END,
			updated_at = NOW()`,
		userID, convID, msgID, isSender)
	return err
}

func (db *DB) GetUpdatedConversations(ctx context.Context, userID int64, since time.Time) ([]*UserConversation, error) {
	rows, err := db.Pool.Query(ctx,
		`SELECT user_id, conversation_id, last_msg_id, unread_count, updated_at
		 FROM user_conversations WHERE user_id=$1 AND updated_at > $2 ORDER BY updated_at DESC`,
		userID, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*UserConversation
	for rows.Next() {
		uc := &UserConversation{}
		if err := rows.Scan(&uc.UserID, &uc.ConversationID, &uc.LastMsgID, &uc.UnreadCount, &uc.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, uc)
	}
	return result, nil
}

func (db *DB) UpdateSyncTime(ctx context.Context, userID int64) error {
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO user_sessions(user_id, last_sync_at) VALUES($1, NOW())
		ON CONFLICT(user_id) DO UPDATE SET last_sync_at = NOW()`, userID)
	return err
}

func (db *DB) GetLastSyncAt(ctx context.Context, userID int64) (time.Time, error) {
	var t time.Time
	err := db.Pool.QueryRow(ctx,
		"SELECT last_sync_at FROM user_sessions WHERE user_id=$1", userID).Scan(&t)
	return t, err
}
