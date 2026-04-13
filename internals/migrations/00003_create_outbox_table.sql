-- +goose Up
CREATE TABLE outbox (
    outbox_key TEXT PRIMARY KEY, 
    transaction_id TEXT NOT NULL, 
    topic TEXT NOT NULL,
    payload BYTEA NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for the Relay Worker
CREATE INDEX idx_outbox_pending ON outbox(created_at) WHERE status = 'PENDING';

-- +goose Down
DROP TABLE IF EXISTS outbox;