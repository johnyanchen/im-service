package model

import "context"

type Group struct {
	ID             int64
	ConversationID int64
	Name           string
	OwnerID        int64
}

func (db *DB) CreateGroup(ctx context.Context, convID int64, name string, ownerID int64) (int64, error) {
	var id int64
	err := db.Pool.QueryRow(ctx,
		"INSERT INTO groups(conversation_id, name, owner_id) VALUES($1, $2, $3) RETURNING id",
		convID, name, ownerID).Scan(&id)
	return id, err
}
