package workers

import (
	"context"
	"log"

	"github.com/swastiijain24/core/internals/kafka"
	pb "github.com/swastiijain24/core/internals/pb"
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

func (w *BankWorker) StartConsumingBankResponse(ctx context.Context) {

	for {
		msg, err := w.bankConsumer.Reader.FetchMessage(ctx)
		if err!= nil{
			break 
		}

		var bankResponse pb.BankResponse

		err = proto.Unmarshal(msg.Value, &bankResponse)
		if err!= nil{
			log.Println("error unpacking message: %v", err)
			continue
		}

		err = w.transactionService.ProcessBankResponse(ctx, bankResponse.GetTransactionId(), bankResponse.GetBankReferenceId(), bankResponse.GetSuccess(), bankResponse.GetErrorMessage(), bankResponse.GetType())
		log.Printf("failed to process bank reponse :%v", err)
		
		if err := w.bankConsumer.Reader.CommitMessages(ctx, msg); err != nil {
                log.Printf("failed to commit: %v", err)
            }

	}
}