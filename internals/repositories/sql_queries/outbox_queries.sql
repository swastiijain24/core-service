-- name: CreateOutboxEntry :execresult
INSERT INTO outbox (
    outbox_key, 
    transaction_id, 
    topic, 
    payload, 
    status
) VALUES (
    $1, $2, $3, $4, 'PENDING'
) 
ON CONFLICT (outbox_key) 
DO NOTHING;

-- name: GetPendingOutboxEntries :many
SELECT 
    outbox_key,
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
SET status = $2,
    updated_at = NOW()
WHERE outbox_key = $1 
  AND status != $2;

-- name: CleanupOutbox :exec
DELETE FROM outbox 
WHERE status = 'SENT' 
AND created_at < NOW() - INTERVAL '24 hours';