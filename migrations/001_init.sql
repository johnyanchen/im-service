CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(64) UNIQUE NOT NULL,
    password_hash VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE conversations (
    id BIGSERIAL PRIMARY KEY,
    type VARCHAR(8) NOT NULL CHECK (type IN ('dm', 'group')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE conversation_members (
    conversation_id BIGINT NOT NULL REFERENCES conversations(id),
    user_id BIGINT NOT NULL REFERENCES users(id),
    PRIMARY KEY (conversation_id, user_id)
);

CREATE TABLE groups (
    id BIGSERIAL PRIMARY KEY,
    conversation_id BIGINT UNIQUE NOT NULL REFERENCES conversations(id),
    name VARCHAR(128) NOT NULL,
    owner_id BIGINT NOT NULL REFERENCES users(id)
);

CREATE TABLE messages (
    id BIGSERIAL PRIMARY KEY,
    conversation_id BIGINT NOT NULL REFERENCES conversations(id),
    from_id BIGINT NOT NULL REFERENCES users(id),
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_messages_conv_id ON messages(conversation_id, id);

CREATE TABLE user_conversations (
    user_id BIGINT NOT NULL REFERENCES users(id),
    conversation_id BIGINT NOT NULL REFERENCES conversations(id),
    last_msg_id BIGINT,
    unread_count INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, conversation_id)
);

CREATE INDEX idx_user_conv_updated ON user_conversations(user_id, updated_at);

CREATE TABLE user_sessions (
    user_id BIGINT PRIMARY KEY REFERENCES users(id),
    last_sync_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
