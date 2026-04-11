package services

import (
	"context"

	pb "github.com/swastiijain24/core/internals/gen"
	"github.com/swastiijain24/core/internals/kafka"
	repo "github.com/swastiijain24/core/internals/repositories"
	"google.golang.org/protobuf/proto"
)

type TransactionService interface {
	NewTransaction(ctx context.Context ,transactionId string, payerAccountId string, payeeAccountId string, amount int64)
}

type txnsvc struct {
	repo repo.Querier
	bankProducer *kafka.Producer

}

func NewTransactionService(repo repo.Querier, bankProducer *kafka.Producer) TransactionService {
	return &txnsvc{
		repo: repo,
		bankProducer: bankProducer,
	}
}

func (s *txnsvc) NewTransaction(ctx context.Context ,transactionId string, payerAccountId string, payeeAccountId string, amount int64) {

	txnParams := repo.CreateTransactionParams{
		TransactionID: transactionId,
		PayerAccountID: payerAccountId,
		PayeeAccountID: payeeAccountId,
		Amount: amount,
		Status: "PENDING",
	}

	_, err :=s.repo.CreateTransaction(ctx, txnParams)
	if err != nil{
		return //err 
	}

	message := &pb.PaymentRequest{
		TransactionId:  transactionId,
		PayerAccountId: payerAccountId,
		PayeeAccountId: payeeAccountId,
		Amount:         amount,
	}

	data, err := proto.Marshal(message)
	if err != nil {
		return //err
	}

	s.bankProducer.ProduceEvent(ctx, transactionId, data)

}
