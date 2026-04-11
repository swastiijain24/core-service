-- name: CreateTransaction :execresult
INSERT INTO transactions (
    transaction_id, 
    payer_account_id, 
    payee_account_id, 
    amount, 
    status
) VALUES (
    $1, $2, $3, $4, $5
) ON CONFLICT (transaction_id) DO NOTHING;

-- name: UpdateTransactionStatus :execrows
UPDATE transactions 
SET 
    status = $2, 
    bank_reference_id = COALESCE($3, bank_reference_id),
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