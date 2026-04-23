package workers

import (
	"context"
	"fmt"
	"log"

	Kafka "github.com/segmentio/kafka-go"
	"github.com/swastiijain24/core/internals/kafka"
	pb "github.com/swastiijain24/core/internals/pb"
	"github.com/swastiijain24/core/internals/services"
	"google.golang.org/protobuf/proto"
)

const maxRetryCount = 5

type PaymentWorker struct {
	paymentConsumer    *kafka.Consumer
	dlqProducer        *kafka.Producer
	transactionService services.TransactionService
	failureCounts      map[string]int
}

func NewPaymentWorker(paymentConsumer *kafka.Consumer, dlqProducer *kafka.Producer, transactionService services.TransactionService) *PaymentWorker {
	return &PaymentWorker{
		paymentConsumer:    paymentConsumer,
		dlqProducer:        dlqProducer,
		transactionService: transactionService,
		failureCounts:      make(map[string]int),
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
			if err := w.moveToDLQ(ctx, msg, "unmarshal error"); err == nil {
				w.clearFailureCount(msg)
			}
			continue
		}
		log.Print("request received by core service 5")

		err = w.transactionService.NewTransaction(ctx, payment.GetTransactionId(), payment.GetPayerAccountId(), payment.GetPayeeAccountId(), payment.GetAmount(), payment.GetPayerBankCode(), payment.GetPayeeBankCode(), payment.GetMpin())
		if err != nil {
			fmt.Println("error starting transaction:", err)
			if err.Error() == "transaction already exists" {
				if err := w.moveToDLQ(ctx, msg, "duplicate transaction"); err == nil {
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

		if err := w.paymentConsumer.Reader.CommitMessages(ctx, msg); err != nil {
			fmt.Println("failed to commit message:", err)
			continue
		}
		w.clearFailureCount(msg)

	}
}

func (w *PaymentWorker) moveToDLQ(ctx context.Context, msg Kafka.Message, reason string) error {
	log.Printf("Moving message %s to DLQ. Reason: %s", string(msg.Key), reason)
	err := w.dlqProducer.ProduceEvent(ctx, string(msg.Key), msg.Value, "bank.response.failed")
	if err != nil {
		log.Printf("DLQ write failed for key=%s: %v", string(msg.Key), err)
		return err
	}

	if err := w.paymentConsumer.Reader.CommitMessages(ctx, msg); err != nil {
		log.Printf("Failed to commit poisoned message: %v", err)
		return err
	}

	return nil
}

func (w *PaymentWorker) messageID(msg Kafka.Message) string {
	return fmt.Sprintf("%s:%d:%d", msg.Topic, msg.Partition, msg.Offset)
}

func (w *PaymentWorker) incrementFailureCount(msg Kafka.Message) int {
	id := w.messageID(msg)
	w.failureCounts[id]++
	return w.failureCounts[id]
}

func (w *PaymentWorker) clearFailureCount(msg Kafka.Message) {
	delete(w.failureCounts, w.messageID(msg))
}
