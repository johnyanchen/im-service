package model

import "context"

type Conversation struct {
	ID   int64
	Type string
}

func (db *DB) CreateConversation(ctx context.Context, convType string) (int64, error) {
	var id int64
	err := db.Pool.QueryRow(ctx,
		"INSERT INTO conversations(type) VALUES($1) RETURNING id",
		convType).Scan(&id)
	return id, err
}

func (db *DB) AddConversationMember(ctx context.Context, convID, userID int64) error {
	_, err := db.Pool.Exec(ctx,
		"INSERT INTO conversation_members(conversation_id, user_id) VALUES($1, $2) ON CONFLICT DO NOTHING",
		convID, userID)
	return err
}

func (db *DB) GetConversationMembers(ctx context.Context, convID int64) ([]int64, error) {
	rows, err := db.Pool.Query(ctx,
		"SELECT user_id FROM conversation_members WHERE conversation_id=$1", convID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var members []int64
	for rows.Next() {
		var uid int64
		if err := rows.Scan(&uid); err != nil {
			return nil, err
		}
		members = append(members, uid)
	}
	return members, nil
}

func (db *DB) IsMember(ctx context.Context, convID, userID int64) (bool, error) {
	var exists bool
	err := db.Pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM conversation_members WHERE conversation_id=$1 AND user_id=$2)",
		convID, userID).Scan(&exists)
	return exists, err
}

func (db *DB) FindDMConversation(ctx context.Context, userA, userB int64) (int64, error) {
	var convID int64
	err := db.Pool.QueryRow(ctx, `
		SELECT cm1.conversation_id FROM conversation_members cm1
		JOIN conversation_members cm2 ON cm1.conversation_id = cm2.conversation_id
		JOIN conversations c ON c.id = cm1.conversation_id
		WHERE cm1.user_id=$1 AND cm2.user_id=$2 AND c.type='dm'
		LIMIT 1`, userA, userB).Scan(&convID)
	return convID, err
}
