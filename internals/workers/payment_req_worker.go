package workers

import (
	"context"
	"fmt"
	Kafka "github.com/segmentio/kafka-go"
	"github.com/swastiijain24/core/internals/kafka"
	pb "github.com/swastiijain24/core/internals/pb"
	"github.com/swastiijain24/core/internals/services"
	"google.golang.org/protobuf/proto"
	"log"
)

type PaymentWorker struct {
	paymentConsumer    *kafka.Consumer
	dlqProducer        *kafka.Producer
	transactionService services.TransactionService
}

func NewPaymentWorker(paymentConsumer *kafka.Consumer, dlqProducer *kafka.Producer, transactionService services.TransactionService) *PaymentWorker {
	return &PaymentWorker{
		paymentConsumer:    paymentConsumer,
		dlqProducer:        dlqProducer,
		transactionService: transactionService,
	}
}

func (w *PaymentWorker) StartConsumingPaymentRequest(ctx context.Context) {
	for {

		msg, err := w.paymentConsumer.Reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Fetch error: %v", err)
			continue
		}

		var payment pb.PaymentRequest

		err = proto.Unmarshal(msg.Value, &payment)
		if err != nil {
			fmt.Println("error unpacking message:", err)
			w.moveToDLQ(ctx, msg, "unmarshal error")
			continue
		}
		log.Print("request received by core service 5")

		err = w.transactionService.NewTransaction(ctx, payment.GetTransactionId(), payment.GetPayerAccountId(), payment.GetPayeeAccountId(), payment.GetAmount(), payment.GetPayerBankCode(), payment.GetPayeeBankCode(), payment.GetMpin())
		if err != nil {
			fmt.Println("error starting transaction:", err)
			if err.Error() == "transaction already exists" {
				w.moveToDLQ(ctx, msg, "duplicate transaction")
			}
			continue
		}

		if err := w.paymentConsumer.Reader.CommitMessages(ctx, msg); err != nil {
			fmt.Println("failed to commit message:", err)
		}

	}
}

func (w *PaymentWorker) moveToDLQ(ctx context.Context, msg Kafka.Message, reason string) {
	log.Printf("Moving message %s to DLQ. Reason: %s", string(msg.Key), reason)
	err := w.dlqProducer.ProduceEvent(ctx, string(msg.Key), msg.Value, "bank.response.failed")
	if err != nil {
		log.Fatalf("Critical Failure: Cannot write to DLQ: %v", err)
	}

	if err := w.paymentConsumer.Reader.CommitMessages(ctx, msg); err != nil {
		log.Printf("Failed to commit poisoned message: %v", err)
	}
}
