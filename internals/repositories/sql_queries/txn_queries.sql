-- name: CreateTransaction :execresult
INSERT INTO transactions (
    transaction_id, 
    payer_account_id, 
    payee_account_id, 
    amount, 
    status,
    payer_bank_code,
    payee_bank_code
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
) ON CONFLICT (transaction_id) DO NOTHING;

-- name: UpdateDebitLeg :execrows
UPDATE transactions 
SET 
    status = $2, 
    debit_bank_ref = COALESCE($3, debit_bank_ref),
    failure_reason = COALESCE($4, failure_reason),
    updated_at = NOW()
WHERE transaction_id = $1;

-- name: UpdateCreditLeg :execrows
UPDATE transactions 
SET 
    status = $2, 
    credit_bank_ref = COALESCE($3, credit_bank_ref),
    failure_reason = COALESCE($4, failure_reason),
    updated_at = NOW()
WHERE transaction_id = $1;

-- name: IncrementRetryCount :exec
UPDATE transactions
SET 
    retry_count = retry_count + 1,
    updated_at = NOW()
WHERE transaction_id = $1;

-- name: GetTransaction :one
SELECT * FROM transactions
WHERE transaction_id = $1 LIMIT 1;