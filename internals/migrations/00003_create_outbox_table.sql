-- +goose Up
CREATE TABLE outbox (
    outbox_key TEXT PRIMARY KEY, -- This will be "uuid_CREDIT", "uuid_FINAL", etc.
    transaction_id TEXT NOT NULL, -- This stays "uuid" (The original Kafka Key)
    topic TEXT NOT NULL,
    payload BYTEA NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for the Relay Worker
CREATE INDEX idx_outbox_pending ON outbox(created_at) WHERE status = 'PENDING';

-- +goose Down
DROP TABLE IF EXISTS outbox;