-- +goose Up
ALTER TABLE transactions 
    RENAME COLUMN bank_reference_id TO debit_bank_ref;

ALTER TABLE transactions 
    ADD COLUMN credit_bank_ref TEXT,
    ADD COLUMN payer_bank_code TEXT,
    ADD COLUMN payee_bank_code TEXT;

-- +goose Down
ALTER TABLE transactions 
    RENAME COLUMN debit_bank_ref TO bank_reference_id;

ALTER TABLE transactions 
    DROP COLUMN credit_bank_ref,
    DROP COLUMN payer_bank_code,
    DROP COLUMN payee_bank_code;
