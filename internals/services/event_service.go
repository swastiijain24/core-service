// package services

// import (
// 	"context"
// 	"fmt"

// 	pb "github.com/swastiijain24/core/internals/gen"
// 	"github.com/swastiijain24/core/internals/kafka"
// 	"google.golang.org/protobuf/proto"
// )

// type EventService interface {
// 	ConsumeTxnReqEvent(context.Context)
// }

// type eventsvc struct {
// 	reqConsumer        *kafka.Consumer
// 	bankReqProducer    *kafka.Producer
// 	transactionService TransactionService
// }

// func NewEventService(reqConsumer *kafka.Consumer, bankReqProducer *kafka.Producer, transactionService TransactionService) EventService {
// 	return &eventsvc{
// 		reqConsumer:        reqConsumer,
// 		transactionService: transactionService,
// 	}
// }

// func (s *eventsvc) ConsumeTxnReqEvent(ctx context.Context) {
// 	for {
// 		msg, err := s.reqConsumer.Reader.ReadMessage(ctx)
// 		if err != nil {
// 			break
// 		}

// 		var payment pb.PaymentRequest

// 		err = proto.Unmarshal(msg.Value, &payment)
// 		if err != nil {
// 			fmt.Println("error unpacking message:", err)
// 			continue
// 		}

// 		s.transactionService.CreateTransaction(payment.TransactionId, payment.PayerAccountId, payment.PayeeAccountId, payment.Amount)
// 		// if err != nil {
// 		// 	return //failed to create txn
// 		// }

// 	}
// }
