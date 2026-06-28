package model

import (
	"context"
	"time"
)

type User struct {
	ID           int64
	Username     string
	PasswordHash string
	CreatedAt    time.Time
}

func (db *DB) CreateUser(ctx context.Context, username, passwordHash string) (int64, error) {
	var id int64
	err := db.Pool.QueryRow(ctx,
		"INSERT INTO users(username, password_hash) VALUES($1, $2) RETURNING id",
		username, passwordHash).Scan(&id)
	return id, err
}

func (db *DB) GetUserByUsername(ctx context.Context, username string) (*User, error) {
	u := &User{}
	err := db.Pool.QueryRow(ctx,
		"SELECT id, username, password_hash, created_at FROM users WHERE username=$1",
		username).Scan(&u.ID, &u.Username, &u.PasswordHash, &u.CreatedAt)
	if err != nil {
		return nil, err
	}
	return u, nil
}

func (db *DB) ListUsers(ctx context.Context) ([]*User, error) {
	rows, err := db.Pool.Query(ctx, "SELECT id, username FROM users ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []*User
	for rows.Next() {
		u := &User{}
		if err := rows.Scan(&u.ID, &u.Username); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, nil
}
