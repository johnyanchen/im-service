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
	// min_visible_msg_id：
	//   - 行已存在（正常收发消息）→ 保持不变（沿用 A 上次删好友设置的下界，或默认 0）。
	//   - 行不存在（删好友后重新加回、视图行重建）→ 设为 (msgID - 1)，即本条消息之前的历史都不可见，
	//     A 只从这条重新聊起的消息开始看得到。用 msgID-1 而非 msgID，保证当前这条自己也可见。
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO user_conversations(user_id, conversation_id, last_msg_id, last_read_msg_id, min_visible_msg_id, last_msg_content, last_msg_from, conv_type, conv_name, updated_at)
		VALUES($1, $2, $3, CASE WHEN $4::bool THEN $3::bigint ELSE 0::bigint END, GREATEST($3::bigint - 1, 0), $5, $6, $7, $8, NOW())
		ON CONFLICT(user_id, conversation_id) DO UPDATE SET
			last_msg_id = $3,
			last_read_msg_id = CASE WHEN $4::bool THEN $3::bigint ELSE user_conversations.last_read_msg_id END,
			last_msg_content = $5,
			last_msg_from = $6,
			conv_type = $7,
			conv_name = $8,
			is_deleted = FALSE,
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
		 WHERE uc.user_id=$1 AND uc.updated_at > $2 AND uc.is_deleted = FALSE
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

// SeedUserConversation 在创建会话时为成员插入一条空的会话视图行，
// 让会话能立即出现在会话列表里（即使还没有任何消息）。
func (db *DB) SeedUserConversation(ctx context.Context, userID, convID int64, convType, convName string) error {
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO user_conversations(user_id, conversation_id, last_msg_id, last_read_msg_id, last_msg_content, last_msg_from, conv_type, conv_name, updated_at)
		VALUES($1, $2, 0, 0, '', '', $3, $4, NOW())
		ON CONFLICT(user_id, conversation_id) DO UPDATE SET
			conv_type = EXCLUDED.conv_type,
			conv_name = EXCLUDED.conv_name`,
		userID, convID, convType, convName)
	return err
}

// RestoreDMConversation 重新加好友后从联系人点"发消息"时调用，
// 把该会话视图行标记为未删除，让会话重新出现在列表里。
// convID 由调用方从 FindDMConversation 拿到，直接按主键更新，无需联表。
func (db *DB) RestoreDMConversation(ctx context.Context, userID, convID int64) error {
	_, err := db.Pool.Exec(ctx,
		`UPDATE user_conversations SET is_deleted = FALSE WHERE user_id=$1 AND conversation_id=$2`,
		userID, convID)
	return err
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

// GetMinVisibleMsgID 返回用户在该会话的可见下界。行不存在时返回 0（可见全部）。
func (db *DB) GetMinVisibleMsgID(ctx context.Context, userID, convID int64) (int64, error) {
	var minID int64
	err := db.Pool.QueryRow(ctx,
		`SELECT min_visible_msg_id FROM user_conversations WHERE user_id=$1 AND conversation_id=$2`,
		userID, convID).Scan(&minID)
	if err != nil {
		return 0, err
	}
	return minID, nil
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
