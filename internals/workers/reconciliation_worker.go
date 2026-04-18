package workers

import (
	"context"
	"time"

	"github.com/swastiijain24/core/internals/kafka"
	"github.com/swastiijain24/core/internals/services"
)

type ReconWorker struct {
	transactionService services.TransactionService
	producer *kafka.Producer
}

func NewReconWorker(transactionService services.TransactionService, producer *kafka.Producer) *ReconWorker {
	return &ReconWorker{
		transactionService: transactionService,
		producer: producer,
	}
}

func (w *ReconWorker) StartWorker(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	for {
		select{
		case <- ticker.C:
			transactions, _ := w.transactionService.GetStuckTransactions(ctx)
			for _, txn := range transactions{
				var txnType string 
				switch txn.Status{
				case "DEBIT_PENDING":
					txnType = "DEBIT"
				case "CREDIT_PENDING":
					txnType = "CREDIT"
				case "REFUNDING":
					txnType = "REFUND"
				}
				w.producer.ProduceEvent(ctx, txn.TransactionID ,[]byte(txnType) ,"bank.enquiry.v1")
			}
		case <- ctx.Done():
			return 
		}
	}
}