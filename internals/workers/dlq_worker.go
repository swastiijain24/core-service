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
	outboxService services.OutboxService
}

func NewDLQWorker(transactionService services.TransactionService, outboxService services.OutboxService, producer *kafka.Producer, consumer *kafka.Consumer) *DLQWorker {
	return &DLQWorker{
		producer: producer,
		consumer: consumer,
		transactionService: transactionService,
		outboxService: outboxService,
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

		transaction, err := w.transactionService.GetTransactionById(ctx, string(msg.Key))
		if err == nil {
			w.transactionService.UpdateTransactionStatus(ctx, transaction.TransactionID, "FAILED")
			w.outboxService.PushToOutbox()
		}
	}	
}
