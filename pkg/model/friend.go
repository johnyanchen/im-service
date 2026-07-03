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

func (db *DB) CreateFriendRequest(ctx context.Context, fromID, toID int64) (int64, error) {
	var id int64
	err := db.Pool.QueryRow(ctx,
		`INSERT INTO friend_requests (from_id, to_id) VALUES ($1, $2) RETURNING id`, fromID, toID).Scan(&id)
	return id, err
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

// DeleteFriend 单向删除语义（对齐微信）：
//   - 双向解除好友关系（friendships 两行都删）。
//   - 只处理发起方 uid 自己的会话视图：把 min_visible_msg_id 推到会话当前最大 msg_id，
//     并把该视图行的 last_read 也一起推平、last_msg 清空——这样列表里那条会话消失，
//     且无论从哪个入口（通讯录发消息命中老会话 / 直接点老会话）重新进来，都看不到旧历史。
//   - conversation_members、对方视图行、消息本身都不动。uid 重新加好友后 findOrCreateDM
//     仍命中老会话，靠这里固定下的 min_visible_msg_id 让 uid 只看得到重新聊起之后的消息。
//
// 关键：下界在“删除时”就固定，不依赖“发首条新消息时重建视图行”，杜绝先点开会话就漏历史。
//
// friendID 是被删的好友。
func (db *DB) DeleteFriend(ctx context.Context, uid, friendID int64) error {
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`DELETE FROM friendships WHERE (user_id=$1 AND friend_id=$2) OR (user_id=$2 AND friend_id=$1)`,
		uid, friendID); err != nil {
		return err
	}
	// 把 uid 与该好友的 dm 会话视图行的可见下界推到会话当前最大 msg_id，
	// 并清空列表展示、推平已读。用 UPDATE 而非 DELETE：保留行以承载 min_visible_msg_id，
	// 避免删行后重建又落回默认 0。GREATEST 兜底不回退已有下界。
	if _, err := tx.Exec(ctx, `
		UPDATE user_conversations uc
		SET min_visible_msg_id = GREATEST(
				uc.min_visible_msg_id,
				COALESCE((SELECT MAX(m.id) FROM messages m WHERE m.conversation_id = uc.conversation_id), 0)
			),
			last_read_msg_id = GREATEST(
				uc.last_read_msg_id,
				COALESCE((SELECT MAX(m.id) FROM messages m WHERE m.conversation_id = uc.conversation_id), 0)
			),
			last_msg_content = '',
			last_msg_from = '',
			is_deleted = TRUE
		WHERE uc.user_id=$1
		  AND uc.conversation_id IN (
			SELECT cm1.conversation_id FROM conversation_members cm1
			JOIN conversation_members cm2 ON cm1.conversation_id = cm2.conversation_id
			JOIN conversations c ON c.id = cm1.conversation_id
			WHERE cm1.user_id=$1 AND cm2.user_id=$2 AND c.type='dm'
		  )`, uid, friendID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
