-- 删好友后重新加回并复用老会话时，A 只应看到"重新加好友之后"的消息。
-- min_visible_msg_id 是该用户在这个会话里可见消息的下界（仅可见 id > 它的消息）。
-- 默认 0 表示可见全部历史；删好友再加回时会被抬高到当时会话的最新 msg_id。
ALTER TABLE user_conversations
    ADD COLUMN min_visible_msg_id BIGINT NOT NULL DEFAULT 0;
