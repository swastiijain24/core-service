-- name: CreateOutboxEntry :execresult
INSERT INTO outbox (
    transaction_id, 
    topic, 
    payload, 
    status
) VALUES (
    $1, $2, $3, 'PENDING'
) 
ON CONFLICT (txn_id) 
DO NOTHING;

-- name: GetPendingOutboxEntries :many
SELECT 
    transaction_id, 
    topic, 
    payload, 
    status, 
    created_at
FROM outbox
WHERE status = 'PENDING'
ORDER BY created_at ASC
LIMIT 100
FOR UPDATE SKIP LOCKED;

-- name: UpdateOutboxStatus :exec
UPDATE outbox
SET status = $2
WHERE transaction_id = $1 
  AND status != $2;

-- name: CleanupOutbox :exec
DELETE FROM outbox 
WHERE status = 'SENT' 
AND created_at < NOW() - INTERVAL '24 hours';