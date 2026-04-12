package services

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/swastiijain24/core/internals/gen"
	"github.com/swastiijain24/core/internals/kafka"
	repo "github.com/swastiijain24/core/internals/repositories"
	"github.com/swastiijain24/core/internals/utils"
	"google.golang.org/protobuf/proto"
)

type TransactionService interface {
	NewTransaction(ctx context.Context, transactionId string, payerAccountId string, payeeAccountId string, amount int64)
	ProcessBankResponse(ctx context.Context, transactionId string, bankReferenceId string, success bool, errorMessage string, txnType string)
	ProduceFinalResponse(ctx context.Context, transactionId string, bankReferenceId string, Status string)
	// UpdateTransactionStatus(ctx context.Context, transactionId string, bankReferenceId string, status string)
	ProduceBankRequest(ctx context.Context, transactionId string, payerAccountId string, payeeAccountId string, amount int64, txnType string)
}

type txnsvc struct {
	db           *pgxpool.Pool
	repo         repo.Querier
	bankProducer *kafka.Producer
}

func NewTransactionService(repo repo.Querier, db *pgxpool.Pool, bankProducer *kafka.Producer) TransactionService {
	return &txnsvc{
		db:           db,
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

	s.ProduceBankRequest(ctx, transactionId, payerAccountId, payeeAccountId, amount, "DEBIT")

}

func (s *txnsvc) ProduceBankRequest(ctx context.Context, transactionId string, payerAccountId string, payeeAccountId string, amount int64, txnType string) {

	message := &pb.BankRequest{
		TransactionId:  transactionId,
		PayerAccountId: payerAccountId,
		PayeeAccountId: payeeAccountId,
		Amount:         amount,
		Type:           txnType,
	}

	data, err := proto.Marshal(message)
	if err != nil {
		return //err
	}

	s.bankProducer.ProduceEvent(ctx, transactionId, data)
}

func (s *txnsvc) ProduceFinalResponse(ctx context.Context, transactionId string, bankReferenceId string, Status string) {
	message := &pb.PaymentResponse{
		TransactionId: transactionId,
		// BankReferenceId: ,

	}

	data, err := proto.Marshal(message)
	if err != nil {
		return //err
	}

	s.bankProducer.ProduceEvent(ctx, transactionId, data)
}

// func (s *txnsvc) ProcessBankResponse(ctx context.Context, transactionId string, bankReferenceId string, success bool, errorMessage string, txnType string) {

// 	transaction, err := s.repo.GetTransaction(ctx, transactionId)
// 	if err != nil {
// 		return //will have to see this
// 	}

// 	switch txnType {
// 	case "DEBIT":
// 		if !success {
// 			s.UpdateTransactionStatus(ctx, transactionId, bankReferenceId, "DEBIT_FAILED")

// 			if transaction.RetryCount < 3 {
// 				s.repo.IncrementRetryCount(ctx, transactionId)
// 				s.ProduceBankRequest(ctx, transactionId, transaction.PayerAccountID, transaction.PayeeAccountID, transaction.Amount, "DEBIT")
// 			} else {
// 				s.ProduceFinalResponse(ctx, transactionId, bankReferenceId, "FAILED")
// 			}
// 		} else {
// 			s.UpdateTransactionStatus(ctx, transactionId, bankReferenceId, "DEBIT_SUCCESS")
// 			s.ProduceBankRequest(ctx, transactionId, transaction.PayerAccountID, transaction.PayeeAccountID, transaction.Amount, "CREDIT")
// 		}

// 	case "CREDIT":
// 		if !success {
// 			s.UpdateTransactionStatus(ctx, transactionId, bankReferenceId, "CREDIT_FAILED")
// 			if transaction.RetryCount < 3 {
// 				s.repo.IncrementRetryCount(ctx, transactionId)
// 				s.ProduceBankRequest(ctx, transactionId, transaction.PayerAccountID, transaction.PayeeAccountID, transaction.Amount, "CREDIT")
// 			} else {
// 				//reversing the debit
// 				s.ProduceBankRequest(ctx, transactionId, transaction.PayerAccountID, transaction.PayeeAccountID, transaction.Amount, "CREDIT")
// 				s.UpdateTransactionStatus(ctx, transactionId, bankReferenceId, "REVERSED")
// 				s.ProduceFinalResponse(ctx, transactionId, bankReferenceId, "FAILED")
// 			}

// 		} else {
// 			s.UpdateTransactionStatus(ctx, transactionId, bankReferenceId, "CREDIT_SUCCESS")
// 			s.ProduceFinalResponse(ctx, transactionId, bankReferenceId, "SUCCESS")
// 		}

// 	default:
// 		// other like get create acc, or check balance
// 		//will do this later
// 	}

// }

func (s *txnsvc) ProcessBankResponse(ctx context.Context, transactionId string, bankReferenceId string, success bool, errorMessage string, txnType string) {

	dbTx, err := s.db.Begin(ctx)
	if err != nil {
		return
	}
	defer dbTx.Rollback(ctx)

	qtx := repo.New(dbTx)

	transaction, err := qtx.GetTransaction(ctx, transactionId)
	if err != nil {
		return
	}

	switch txnType {

	case "DEBIT":
		if success {
			s.handleDebitSuccess(ctx, qtx, transaction, bankReferenceId)
		} else {
			s.handleDebitFailure(ctx, qtx, transaction, bankReferenceId)
		}

	case "CREDIT":
		if success {
			s.handleCreditSuccess(ctx, qtx, transaction, bankReferenceId)
		} else {
			s.handleCreditFailure(ctx, qtx, transaction, bankReferenceId)
		}
		// default
	}

	dbTx.Commit(ctx)
}

func (s *txnsvc) handleCreditFailure(ctx context.Context, qtx *repo.Queries, transaction repo.Transaction, bankReferenceId string) {
	panic("unimplemented")
}

func (s *txnsvc) handleDebitFailure(ctx context.Context, qtx *repo.Queries, transaction repo.Transaction, bankReferenceId string) {
	panic("unimplemented")
}

func (s *txnsvc) handleDebitSuccess(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string) {

	qtx.UpdateTransactionStatus(ctx, repo.UpdateTransactionStatusParams{
		TransactionID:   transaction.TransactionID,
		BankReferenceID: utils.ToPGText(bankReferenceId),
		Status:          "DEBIT_SUCCESS",
	})

	creditReq := &pb.BankRequest{
		TransactionId:  transaction.TransactionID,
		Type:           "CREDIT",
		PayerAccountId: transaction.PayerAccountID,
		PayeeAccountId: transaction.PayeeAccountID,
		Amount:         transaction.Amount,
	}

	payload, _ := proto.Marshal(creditReq)

	qtx.CreateOutboxEntry(ctx, repo.CreateOutboxEntryParams{
		TransactionID: transaction.TransactionID + "_CREDIT",
		Topic:         "bank.instruction.v1",
		Payload:       payload,
	})

}

func (s *txnsvc) handleCreditSuccess(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string) {
	qtx.UpdateTransactionStatus(ctx, repo.UpdateTransactionStatusParams{
		TransactionID:   transaction.TransactionID,
		BankReferenceID: transaction.BankReferenceID,
		Status:          "CREDIT_SUCCESS",
	})

	finalResponse := &pb.PaymentResponse{
		TransactionId:   transaction.TransactionID,
		BankReferenceId: bankReferenceId,
		Status:          "SUCCESS",
	}

	payload, _ := proto.Marshal(finalResponse)

	qtx.CreateOutboxEntry(ctx, repo.CreateOutboxEntryParams{
		TransactionID: transaction.TransactionID + "_FINAL",
		Topic:         "bank.instruction.v1",
		Payload:       payload,
	})
}
