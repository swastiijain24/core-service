package workers

import (
	"context"
	"log"
	"time"

	"github.com/swastiijain24/core/internals/kafka"
	repo "github.com/swastiijain24/core/internals/repositories"
)

type RelayWorker struct {
	repo repo.Querier
	producer *kafka.Producer
}

func NewRelayWorker(repo repo.Querier, producer *kafka.Producer) *RelayWorker {
	return &RelayWorker{
		repo: repo,
		producer: producer,
	}
}

func (w *RelayWorker) StartRelayingOutboxEntries(ctx context.Context) {

	//polling every 200ms for low latency recovery
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	log.Println("Relay worker started")

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			w.processOutbox(ctx)
		}
	}
}

func (w *RelayWorker) processOutbox(ctx context.Context){

	entries, err := w.repo.GetPendingOutboxEntries(ctx)
	if err != nil {
		log.Printf("Relay: failed to fetch outbox: %v", err)
		return
	}

	for _, entry := range entries{
		log.Print("processing outbox entries")
		err := w.producer.ProduceEvent(ctx, entry.TransactionID, entry.Payload, entry.Topic)
		log.Print("bank request produced from core service")

		if err != nil {
			log.Printf("Relay: failed to publish txn %s: %v", entry.TransactionID, err)
			continue 
		}

		err = w.repo.UpdateOutboxStatus(ctx, repo.UpdateOutboxStatusParams{
			OutboxKey: entry.OutboxKey,
			Status: "SENT",
		})

		if err != nil {
			log.Printf("Relay: failed to update status for %s: %v", entry.TransactionID, err)
		}
	}
}

func (w *RelayWorker) StartCleanupOutbox(ctx context.Context){
	ticker := time.NewTicker(10 * time.Hour)
	defer ticker.Stop()

	for {
		select{
		case <-ctx.Done(): 
		return 
		case <-ticker.C	:
			err := w.repo.CleanupOutbox(ctx)
			if err != nil{
				log.Printf("Cleanup: failed to prune outbox: %v", err)
            } else {
                log.Println("Cleanup: successfully pruned old SENT entries")
            }
			}
		}
	}


