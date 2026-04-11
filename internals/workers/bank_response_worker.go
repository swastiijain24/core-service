package workers

import (
	"context"
	"fmt"

	pb "github.com/swastiijain24/core/internals/gen"
	"github.com/swastiijain24/core/internals/kafka"
	"github.com/swastiijain24/core/internals/services"
	"google.golang.org/protobuf/proto"
)

type BankWorker struct {
	bankConsumer *kafka.Consumer
	transactionService services.TransactionService
}

func NewBankWorker(bankConsumer *kafka.Consumer, transactionService services.TransactionService) *BankWorker {
	return &BankWorker{
		bankConsumer: bankConsumer,
		transactionService: transactionService,
	}
}

func (w *BankWorker) Start(ctx context.Context) {

	for {
		msg, err := w.bankConsumer.Reader.ReadMessage(ctx)
		if err!= nil{
			break 
		}

		var bankResponse pb.BankResponse

		err = proto.Unmarshal(msg.Value, &bankResponse)
		if err!= nil{
			fmt.Println("error unpacking message:", err)
			continue
		}

		w.transactionService.ProcessBankResponse(ctx, bankResponse.GetTransactionId(), bankResponse.GetBankReferenceId(), bankResponse.GetSuccess(), bankResponse.GetErrorMessage())
		
		
	}
}