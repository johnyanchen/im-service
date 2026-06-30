package model

import (
	"context"
)

type FriendRequest struct {
	ID           int64
	FromID       int64
	ToID         int64
	FromUsername string
	Status       string
	CreatedAt    int64
}

func (db *DB) CreateFriendRequest(ctx context.Context, fromID, toID int64) error {
	_, err := db.Pool.Exec(ctx,
		`INSERT INTO friend_requests (from_id, to_id) VALUES ($1, $2)`, fromID, toID)
	return err
}

func (db *DB) GetFriendRequest(ctx context.Context, id int64) (*FriendRequest, error) {
	var r FriendRequest
	err := db.Pool.QueryRow(ctx,
		`SELECT id, from_id, to_id, status FROM friend_requests WHERE id=$1`, id).
		Scan(&r.ID, &r.FromID, &r.ToID, &r.Status)
	return &r, err
}

func (db *DB) AcceptFriendRequest(ctx context.Context, id, fromID, toID int64) error {
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `UPDATE friend_requests SET status='accepted' WHERE id=$1`, id)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO friendships (user_id, friend_id) VALUES ($1,$2),($2,$1) ON CONFLICT DO NOTHING`, fromID, toID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (db *DB) RejectFriendRequest(ctx context.Context, id int64) error {
	_, err := db.Pool.Exec(ctx, `UPDATE friend_requests SET status='rejected' WHERE id=$1`, id)
	return err
}

func (db *DB) ListFriends(ctx context.Context, userID int64) ([]User, error) {
	rows, err := db.Pool.Query(ctx,
		`SELECT u.id, u.username FROM friendships f JOIN users u ON u.id=f.friend_id WHERE f.user_id=$1`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var friends []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Username); err != nil {
			return nil, err
		}
		friends = append(friends, u)
	}
	return friends, nil
}

func (db *DB) ListFriendRequests(ctx context.Context, userID int64) ([]FriendRequest, error) {
	rows, err := db.Pool.Query(ctx,
		`SELECT fr.id, fr.from_id, u.username, fr.status, EXTRACT(EPOCH FROM fr.created_at)::bigint
		 FROM friend_requests fr JOIN users u ON u.id=fr.from_id
		 WHERE fr.to_id=$1 ORDER BY fr.created_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var reqs []FriendRequest
	for rows.Next() {
		var r FriendRequest
		if err := rows.Scan(&r.ID, &r.FromID, &r.FromUsername, &r.Status, &r.CreatedAt); err != nil {
			return nil, err
		}
		reqs = append(reqs, r)
	}
	return reqs, nil
}

func (db *DB) AreFriends(ctx context.Context, a, b int64) (bool, error) {
	var exists bool
	err := db.Pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM friendships WHERE user_id=$1 AND friend_id=$2)`, a, b).Scan(&exists)
	return exists, err
}
