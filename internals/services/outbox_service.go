package services

import (
	"context"

	repo "github.com/swastiijain24/core/internals/repositories"
)

type OutboxService interface {
	PushToOutbox(ctx context.Context, qtx repo.Querier, outboxKey string, transactionId string, topic string, payload []byte) error 
	GetPendingOutboxEntries(ctx context.Context) ([]repo.Outbox, error)
	UpdateOutboxStatus(ctx context.Context, outboxKey string, status string) error
	CleanupOutbox(ctx context.Context) error
}

type OutboxSvc struct {
	repo repo.Querier
}

func NewOutboxService(repo repo.Querier) OutboxService {
	return &OutboxSvc{
		repo: repo,
	}
}


func (s *OutboxSvc) PushToOutbox(ctx context.Context, qtx repo.Querier, outboxKey string, transactionId string, topic string, payload []byte) error {
	_, err := qtx.CreateOutboxEntry(ctx, repo.CreateOutboxEntryParams{
		OutboxKey:     outboxKey,
		TransactionID: transactionId,
		Topic:         topic,
		Payload:       payload,
	})
	if err != nil {
		return err
	}
	return nil
}


func (s *OutboxSvc) GetPendingOutboxEntries(ctx context.Context) ([]repo.Outbox, error) {
	return s.repo.GetPendingOutboxEntries(ctx)
}

func (s *OutboxSvc) UpdateOutboxStatus(ctx context.Context, outboxKey string, status string) error {
	return s.repo.UpdateOutboxStatus(ctx, repo.UpdateOutboxStatusParams{
		OutboxKey: outboxKey,
		Status:    status,
	})
}

func (s *OutboxSvc) CleanupOutbox(ctx context.Context) error {
	return s.repo.CleanupOutbox(ctx)
}