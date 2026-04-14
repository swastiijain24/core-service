package workers

import (
	"context"
	"time"

	"github.com/swastiijain24/core/internals/kafka"
	repo "github.com/swastiijain24/core/internals/repositories"
)

type ReconWorker struct {
	repo repo.Querier 
	producer *kafka.Producer
}

func NewReconWorker(repo repo.Querier, producer *kafka.Producer) *ReconWorker {
	return &ReconWorker{
		repo: repo,
		producer: producer,
	}
}

func (w *ReconWorker) StartWorker(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	for {
		select{
		case <- ticker.C:
			transactions , _ := w.repo.GetStuckTransactions(ctx)
			for _, txn := range transactions{
				var value []byte 
				w.producer.ProduceEvent(ctx, txn.TransactionID,value,"bank.enquiry.v1")
			}
		case <- ctx.Done():
			return 
		}
	}
}