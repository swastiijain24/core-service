-- +goose Up
CREATE TABLE outbox (
    transaction_id VARCHAR(50) PRIMARY KEY, 
    topic TEXT NOT NULL,
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'PENDING'
);

CREATE INDEX idx_outbox_pending ON outbox(created_at) WHERE status = 'PENDING';