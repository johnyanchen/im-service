package model

import (
	"context"
	"time"
)

type UserConversation struct {
	UserID         int64
	ConversationID int64
	LastMsgID      int64
	LastReadMsgID  int64
	UnreadCount    int32 // 读时计算，不存储
	UpdatedAt      time.Time
	Type           string
	Name           string
	LastMsgContent string
	LastMsgFrom    string
}

func (db *DB) UpsertUserConversation(ctx context.Context, userID, convID, msgID int64, isSender bool, content, fromUsername, convType, convName string) error {
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO user_conversations(user_id, conversation_id, last_msg_id, last_read_msg_id, last_msg_content, last_msg_from, conv_type, conv_name, updated_at)
		VALUES($1, $2, $3, CASE WHEN $4::bool THEN $3::bigint ELSE 0::bigint END, $5, $6, $7, $8, NOW())
		ON CONFLICT(user_id, conversation_id) DO UPDATE SET
			last_msg_id = $3,
			last_read_msg_id = CASE WHEN $4::bool THEN $3::bigint ELSE user_conversations.last_read_msg_id END,
			last_msg_content = $5,
			last_msg_from = $6,
			conv_type = $7,
			conv_name = $8,
			updated_at = NOW()`,
		userID, convID, msgID, isSender, content, fromUsername, convType, convName)
	return err
}

func (db *DB) GetUpdatedConversations(ctx context.Context, userID int64, since time.Time, limit int32) ([]*UserConversation, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := db.Pool.Query(ctx,
		`SELECT uc.user_id, uc.conversation_id, uc.last_msg_id, uc.last_read_msg_id, uc.updated_at,
		        uc.conv_type, uc.conv_name, uc.last_msg_content, uc.last_msg_from,
		        (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = uc.conversation_id AND m.id > uc.last_read_msg_id) AS unread_count
		 FROM user_conversations uc
		 WHERE uc.user_id=$1 AND uc.updated_at > $2
		 ORDER BY uc.updated_at DESC LIMIT $3`,
		userID, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*UserConversation
	for rows.Next() {
		uc := &UserConversation{}
		if err := rows.Scan(&uc.UserID, &uc.ConversationID, &uc.LastMsgID, &uc.LastReadMsgID, &uc.UpdatedAt,
			&uc.Type, &uc.Name, &uc.LastMsgContent, &uc.LastMsgFrom, &uc.UnreadCount); err != nil {
			return nil, err
		}
		result = append(result, uc)
	}
	return result, nil
}

func (db *DB) UpdateSyncTime(ctx context.Context, userID int64) error {
	return nil
}

func (db *DB) GetLastSyncAt(ctx context.Context, userID int64) (time.Time, error) {
	return time.Time{}, nil
}

func (db *DB) MarkRead(ctx context.Context, userID, convID, msgID int64) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE user_conversations SET last_read_msg_id = $3
		WHERE user_id = $1 AND conversation_id = $2 AND last_read_msg_id < $3`,
		userID, convID, msgID)
	return err
}

type ConversationMeta struct {
	Type string
	Name string
}

func (db *DB) GetConversationMeta(ctx context.Context, convID, senderID int64) (*ConversationMeta, error) {
	meta := &ConversationMeta{}
	err := db.Pool.QueryRow(ctx, `
		SELECT c.type,
		       COALESCE(g.name, peer.username, '') AS name
		FROM conversations c
		LEFT JOIN groups g ON g.conversation_id = c.id
		LEFT JOIN conversation_members cm_peer
		  ON cm_peer.conversation_id = c.id AND cm_peer.user_id != $2 AND c.type = 'dm'
		LEFT JOIN users peer ON peer.id = cm_peer.user_id
		WHERE c.id = $1
		LIMIT 1`, convID, senderID).Scan(&meta.Type, &meta.Name)
	return meta, err
}
