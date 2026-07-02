package model

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

type User struct {
	ID           int64
	Username     string
	PasswordHash string
	InviteCode   string
	CreatedAt    time.Time
}

func generateInviteCode() string {
	b := make([]byte, 4)
	rand.Read(b)
	return strings.ToUpper(hex.EncodeToString(b))[:6]
}

func (db *DB) CreateUser(ctx context.Context, username, passwordHash string) (int64, error) {
	for i := 0; i < 5; i++ {
		var id int64
		code := generateInviteCode()
		err := db.Pool.QueryRow(ctx,
			"INSERT INTO users(username, password_hash, invite_code) VALUES($1, $2, $3) RETURNING id",
			username, passwordHash, code).Scan(&id)
		if err == nil {
			return id, nil
		}
		if !strings.Contains(err.Error(), "invite_code") {
			return 0, err
		}
	}
	return 0, fmt.Errorf("生成邀请码失败，请重试")
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

func (db *DB) GetUserByID(ctx context.Context, id int64) (*User, error) {
	u := &User{}
	err := db.Pool.QueryRow(ctx,
		"SELECT id, username FROM users WHERE id=$1", id).Scan(&u.ID, &u.Username)
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

func (db *DB) GetInviteCode(ctx context.Context, userID int64) (string, error) {
	var code string
	err := db.Pool.QueryRow(ctx, "SELECT invite_code FROM users WHERE id=$1", userID).Scan(&code)
	return code, err
}

func (db *DB) GetUserByInviteCode(ctx context.Context, code string) (*User, error) {
	u := &User{}
	err := db.Pool.QueryRow(ctx,
		"SELECT id, username FROM users WHERE invite_code=$1",
		strings.ToUpper(code)).Scan(&u.ID, &u.Username)
	if err != nil {
		return nil, err
	}
	return u, nil
}

func (db *DB) RefreshInviteCode(ctx context.Context, userID int64) (string, error) {
	for i := 0; i < 5; i++ {
		code := generateInviteCode()
		_, err := db.Pool.Exec(ctx, "UPDATE users SET invite_code=$1 WHERE id=$2", code, userID)
		if err == nil {
			return code, nil
		}
		if !strings.Contains(err.Error(), "invite_code") {
			return "", err
		}
	}
	return "", fmt.Errorf("生成邀请码失败，请重试")
}
