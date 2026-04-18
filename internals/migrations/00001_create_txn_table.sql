-- +goose Up
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(100) PRIMARY KEY, 
    payer_account_id VARCHAR(100) NOT NULL,
    payee_account_id VARCHAR(100) NOT NULL,
    amount BIGINT NOT NULL,       
    
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    
    retry_count INTEGER DEFAULT 0,
    
    bank_reference_id VARCHAR(100),
    failure_reason TEXT,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_transactions_status ON transactions(status);

-- +goose Down
DROP TABLE IF EXISTS transactions;