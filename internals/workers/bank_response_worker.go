package workers

import (
	"context"
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
	bankProducer       *kafka.Producer
	transactionService services.TransactionService
}

func NewBankWorker(bankConsumer *kafka.Consumer, bankProducer *kafka.Producer, transactionService services.TransactionService) *BankWorker {
	return &BankWorker{
		bankConsumer:       bankConsumer,
		bankProducer: bankProducer,
		transactionService: transactionService,
	}
}

func (w *BankWorker) StartConsumingBankResponse(ctx context.Context) {

	for {

		msg, err := w.bankConsumer.Reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}

		var bankResponse pb.BankResponse

		err = proto.Unmarshal(msg.Value, &bankResponse)
		if err != nil {
			log.Printf("unrecoverable error :error unpacking message: %v", err)
			w.moveToDLQ(ctx, msg, "unmarshal error")
			continue
		}

		log.Print("processing the bank response started")
		err = w.transactionService.ProcessBankResponse(ctx, bankResponse.GetTransactionId(), bankResponse.GetBankReferenceId(), bankResponse.GetSuccess(), bankResponse.GetErrorMessage(), bankResponse.GetType())
		if err != nil {
			log.Printf("failed to process bank reponse :%v", err)

			if isPermanentError(err) {
				w.moveToDLQ(ctx, msg, err.Error())
				continue
			}

			continue
		}
		log.Print("processed the  bank response")

		if err := w.bankConsumer.Reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("failed to commit: %v", err)
		}

	}
}

func (w *BankWorker) moveToDLQ(ctx context.Context, msg Kafka.Message, reason string) {
	log.Printf("Moving message %s to DLQ. Reason: %s", string(msg.Key), reason)
	err := w.bankProducer.ProduceEvent(ctx, string(msg.Key), msg.Value, "bank.response.failed")
	if err != nil {
		log.Fatalf("Critical Failure: Cannot write to DLQ: %v", err)
	}

	if err := w.bankConsumer.Reader.CommitMessages(ctx, msg); err != nil {
		log.Printf("Failed to commit poisoned message: %v", err)
	}
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
