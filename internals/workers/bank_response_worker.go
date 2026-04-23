package workers

import (
	"context"
	"fmt"
	"log"

	"errors"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	Kafka "github.com/segmentio/kafka-go"
	"github.com/swastiijain24/core/internals/kafka"
	pb "github.com/swastiijain24/core/internals/pb"
	"github.com/swastiijain24/core/internals/services"
	"google.golang.org/protobuf/proto"
)

type BankWorker struct {
	bankConsumer       *kafka.Consumer
	dlqProducer        *kafka.Producer
	transactionService services.TransactionService
	failureCounts      map[string]int
}

func NewBankWorker(bankConsumer *kafka.Consumer, dlqProducer *kafka.Producer, transactionService services.TransactionService) *BankWorker {
	return &BankWorker{
		bankConsumer:       bankConsumer,
		dlqProducer:        dlqProducer,
		transactionService: transactionService,
		failureCounts:      make(map[string]int),
	}
}

func (w *BankWorker) StartConsumingBankResponse(ctx context.Context) {

	for {

		msg, err := w.bankConsumer.Reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("fetch error: %v", err)
			continue
		}

		var bankResponse pb.BankResponse

		err = proto.Unmarshal(msg.Value, &bankResponse)
		if err != nil {
			log.Printf("unrecoverable error :error unpacking message: %v", err)
			if err := w.moveToDLQ(ctx, msg, "unmarshal error"); err == nil {
				w.clearFailureCount(msg)
			}
			continue
		}

		log.Print("processing the bank response started")

		err = w.transactionService.ProcessBankResponse(ctx, bankResponse.GetTransactionId(), bankResponse.GetBankReferenceId(), bankResponse.GetSuccess(), bankResponse.GetErrorMessage(), bankResponse.GetType())
		if err != nil {
			log.Printf("failed to process bank response :%v", err)

			if isPermanentError(err) {
				if err := w.moveToDLQ(ctx, msg, err.Error()); err == nil {
					w.clearFailureCount(msg)
				}
				continue
			}

			attempt := w.incrementFailureCount(msg)
			if attempt >= maxRetryCount {
				reason := fmt.Sprintf("transient retries exceeded (%d): %v", attempt, err)
				if err := w.moveToDLQ(ctx, msg, reason); err == nil {
					w.clearFailureCount(msg)
				}
				continue
			}

			log.Printf("transient processing error for key=%s attempt=%d/%d: %v", string(msg.Key), attempt, maxRetryCount, err)

			continue
		}
		log.Print("processed the bank response 18")
		log.Print("processed the bank response 26")
		if err := w.bankConsumer.Reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("failed to commit: %v", err)
			continue
		}
		w.clearFailureCount(msg)

	}
}

func (w *BankWorker) moveToDLQ(ctx context.Context, msg Kafka.Message, reason string) error {
	log.Printf("%s failed due to Reason: %s", string(msg.Key), reason)

	err := w.dlqProducer.ProduceEvent(ctx, string(msg.Key), msg.Value, "bank.response.failed")
	if err != nil {
		log.Printf("DLQ write failed for key=%s: %v", string(msg.Key), err)
		return err
	}

	if err := w.bankConsumer.Reader.CommitMessages(ctx, msg); err != nil {
		log.Printf("Failed to commit poisoned message: %v", err)
		return err
	}

	return nil
}

func (w *BankWorker) messageID(msg Kafka.Message) string {
	return fmt.Sprintf("%s:%d:%d", msg.Topic, msg.Partition, msg.Offset)
}

func (w *BankWorker) incrementFailureCount(msg Kafka.Message) int {
	id := w.messageID(msg)
	w.failureCounts[id]++
	return w.failureCounts[id]
}

func (w *BankWorker) clearFailureCount(msg Kafka.Message) {
	delete(w.failureCounts, w.messageID(msg))
}

func isPermanentError(err error) bool {
	if err == nil {
		return false
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case pgerrcode.ForeignKeyViolation, // 23503: TxnID doesn't exist
			pgerrcode.InvalidTextRepresentation, // 22P02: Bad UUID format
			pgerrcode.NotNullViolation:          // 23502: Missing required field
			return true
		}
	}

	return false
}
