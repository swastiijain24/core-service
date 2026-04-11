package workers

import (
	"context"
	"fmt"

	pb "github.com/swastiijain24/core/internals/gen"
	"github.com/swastiijain24/core/internals/kafka"
	"github.com/swastiijain24/core/internals/services"
	"google.golang.org/protobuf/proto"
)

type PaymentWorker struct {
	paymentConsumer *kafka.Consumer
	transactionService services.TransactionService
}

func NewPaymentWorker(paymenrConsumer *kafka.Consumer, transactionService services.TransactionService) *PaymentWorker {
	return &PaymentWorker{
		paymentConsumer: paymenrConsumer,
		transactionService: transactionService,
	}
}

func (w *PaymentWorker) Start(ctx context.Context) {
	for {
		msg, err := w.paymentConsumer.Reader.ReadMessage(ctx)
		if err != nil {
			break
		}

		var payment pb.PaymentRequest

		err = proto.Unmarshal(msg.Value, &payment)
		if err != nil {
			fmt.Println("error unpacking message:", err)
			continue
		}

		w.transactionService.NewTransaction(ctx, payment.GetTransactionId(), payment.GetPayerAccountId(), payment.GetPayeeAccountId(), payment.GetAmount())
		

	}
}
