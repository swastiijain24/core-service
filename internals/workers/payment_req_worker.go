package workers

import (
	"context"
	"fmt"
	"log"

	"github.com/swastiijain24/core/internals/kafka"
	pb "github.com/swastiijain24/core/internals/pb"
	"github.com/swastiijain24/core/internals/services"
	"google.golang.org/protobuf/proto"
)

type PaymentWorker struct {
	paymentConsumer    *kafka.Consumer
	transactionService services.TransactionService
}

func NewPaymentWorker(paymentConsumer *kafka.Consumer, transactionService services.TransactionService) *PaymentWorker {
	return &PaymentWorker{
		paymentConsumer:    paymentConsumer,
		transactionService: transactionService,
	}
}

func (w *PaymentWorker) StartConsumingPaymentRequest(ctx context.Context) {
	for {


		msg, err := w.paymentConsumer.Reader.FetchMessage(ctx)
		if err != nil {
			fmt.Println("error fetching message:", err)
			break
		}

		var payment pb.PaymentRequest

		err = proto.Unmarshal(msg.Value, &payment)
		if err != nil {
			fmt.Println("error unpacking message:", err)
			continue
		}

		log.Print("request received by core service")

		err = w.transactionService.NewTransaction(ctx, payment.GetTransactionId(), payment.GetPayerAccountId(), payment.GetPayeeAccountId(), payment.GetAmount(), payment.GetPayerBankCode(), payment.GetPayeeBankCode(), payment.GetMpin())
		if err != nil {
			fmt.Println("error starting transaction:", err)
		}

		if err := w.paymentConsumer.Reader.CommitMessages(ctx, msg); err != nil {
			fmt.Println("failed to commit message:", err)
		}
		
	}
}
