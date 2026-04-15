package workers

import (
	"context"
	"log"

	"github.com/swastiijain24/core/internals/kafka"
	"github.com/swastiijain24/core/internals/services"
)

type DLQWorker struct {
	producer *kafka.Producer
	consumer *kafka.Consumer
	transactionService services.TransactionService
}

func NewDLQWorker(transactionService services.TransactionService, producer *kafka.Producer, consumer *kafka.Consumer) *DLQWorker {
	return &DLQWorker{
		producer: producer,
		consumer: consumer,
		transactionService: transactionService,
	}
}

func (w *DLQWorker) StartDLQWorker(ctx context.Context) {
	for {
		msg, err := w.consumer.Reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("fetch error: %v", err)
			continue
		}

		transactionId := string(msg.Key)
		err = w.transactionService.MarkAsFailedWithOutBox(ctx, transactionId, "system_error")

		if err != nil {
			log.Printf("Failed to resolve DLQ message for txn %s: %v", transactionId, err)
            continue
		}
		if err := w.consumer.Reader.CommitMessages(ctx, msg); err != nil {
            log.Printf("Failed to commit DLQ message: %v", err)
        }
	}	
}
