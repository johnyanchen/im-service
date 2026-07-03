ALTER TABLE user_conversations
    ADD COLUMN is_deleted BOOLEAN NOT NULL DEFAULT FALSE;


psql "postgres://im:im@localhost:5432/im?sslmode=disable"