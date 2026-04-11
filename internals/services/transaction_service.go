package services

import (
	"context"

	pb "github.com/swastiijain24/core/internals/gen"
	"github.com/swastiijain24/core/internals/kafka"
	repo "github.com/swastiijain24/core/internals/repositories"
	"google.golang.org/protobuf/proto"
)

type TransactionService interface {
	NewTransaction(ctx context.Context, transactionId string, payerAccountId string, payeeAccountId string, amount int64)
	ProcessBankResponse(ctx context.Context, transactionId string, bankReferenceId string, success bool, errorMessage string)
}

type txnsvc struct {
	repo         repo.Querier
	bankProducer *kafka.Producer
}

func NewTransactionService(repo repo.Querier, bankProducer *kafka.Producer) TransactionService {
	return &txnsvc{
		repo:         repo,
		bankProducer: bankProducer,
	}
}

func (s *txnsvc) NewTransaction(ctx context.Context, transactionId string, payerAccountId string, payeeAccountId string, amount int64) {

	txnParams := repo.CreateTransactionParams{
		TransactionID:  transactionId,
		PayerAccountID: payerAccountId,
		PayeeAccountID: payeeAccountId,
		Amount:         amount,
		Status:         "DEBIT_PENDING",
	}

	_, err := s.repo.CreateTransaction(ctx, txnParams)
	if err != nil {
		return //err
	}

	message := &pb.BankRequest{
		TransactionId:  transactionId,
		PayerAccountId: payerAccountId,
		PayeeAccountId: payeeAccountId,
		Amount:         amount,
		Type:           pb.TransactionType_TXN_TYPE_DEBIT,
	}

	data, err := proto.Marshal(message)
	if err != nil {
		return //err
	}

	s.bankProducer.ProduceEvent(ctx, transactionId, data)

}

func (s *txnsvc) ProcessBankResponse(ctx context.Context, transactionId string, bankReferenceId string, success bool, errorMessage string) {
	if !success {
		//will retry
	} else {

		transaction, err := s.repo.GetTransaction(ctx, transactionId)
		if err != nil {
			return //will have to see this
		}

		//create the credit event
		message := &pb.BankRequest{
			TransactionId:  transactionId,
			PayerAccountId: transaction.PayerAccountID,
			PayeeAccountId: transaction.PayerAccountID,
			Amount:         transaction.Amount,
			Type:           pb.TransactionType_TXN_TYPE_CREDIT,
		}

		data, err := proto.Marshal(message)
		if err != nil {
			return //err
		}

		s.bankProducer.ProduceEvent(ctx, transactionId, data)

	}
}
