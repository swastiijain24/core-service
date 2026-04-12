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
	handleCreditFailure(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string)
	handleCreditSuccess(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string)
	handleDebitFailure(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string)
	handleDebitSuccess(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string)
	pushToOutBox(ctx context.Context, qtx repo.Querier, transactionId string, topic string, payload []byte)
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

	dbTx, err := s.db.Begin(ctx)
	if err != nil {
		return
	}
	defer dbTx.Rollback(ctx)

	qtx := repo.New(dbTx)

	_, err = qtx.CreateTransaction(ctx, repo.CreateTransactionParams{
		TransactionID:  transactionId,
		PayerAccountID: payerAccountId,
		PayeeAccountID: payeeAccountId,
		Amount:         amount,
		Status:         "DEBIT_PENDING",
	})
	if err != nil {
		return //err
	}

	message := &pb.BankRequest{
		TransactionId:  transactionId,
		PayerAccountId: payerAccountId,
		PayeeAccountId: payeeAccountId,
		Amount:         amount,
		Type:           "DEBIT",
	}

	payload, err := proto.Marshal(message)
	if err != nil {
		return //err
	}

	s.pushToOutBox(ctx, qtx, transactionId, "bank.instruction.v1", payload)

	dbTx.Commit(ctx)

}

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


func (s *txnsvc) handleCreditFailure(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string) {

	if transaction.RetryCount < 3 {
		qtx.UpdateTransactionStatus(ctx, repo.UpdateTransactionStatusParams{
			TransactionID: transaction.TransactionID,
			Status:        "CREDIT_PENDING",
		})
		s.repo.IncrementRetryCount(ctx, transaction.TransactionID)

		retryReq := &pb.BankRequest{
			TransactionId:  transaction.TransactionID,
			Type:           "CREDIT",
			PayerAccountId: transaction.PayerAccountID,
			PayeeAccountId: transaction.PayeeAccountID, // will have to swap them
			Amount:         transaction.Amount,
		}

		payload, _ := proto.Marshal(retryReq)

		s.pushToOutBox(ctx, qtx, transaction.TransactionID+"_RETRY"+string(transaction.RetryCount.Int32), "bank.instruction.v1", payload)

		return
	}

	qtx.UpdateTransactionStatus(ctx, repo.UpdateTransactionStatusParams{
		TransactionID:   transaction.TransactionID,
		BankReferenceID: utils.ToPGText(bankReferenceId),
		Status:          "CREDIT_FAILED",
	})

	refundReq := &pb.BankRequest{
		TransactionId:  transaction.TransactionID,
		Type:           "CREDIT",
		PayerAccountId: "SETTLEMENT_ACCOUNT",
		PayeeAccountId: transaction.PayerAccountID,
		Amount:         transaction.Amount,
	}

	payload, _ := proto.Marshal(refundReq)

	s.pushToOutBox(ctx, qtx, transaction.TransactionID+"_REFUND", "bank.instruction.v1", payload)
}

func (s *txnsvc) handleDebitFailure(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string) {
	qtx.UpdateTransactionStatus(ctx, repo.UpdateTransactionStatusParams{
		TransactionID:   transaction.TransactionID,
		BankReferenceID: transaction.BankReferenceID,
		Status:          "DEBIT_FAILED",
	})

	finalResponse := &pb.PaymentResponse{
		TransactionId:   transaction.TransactionID,
		BankReferenceId: bankReferenceId,
		Status:          "FAILED",
	}

	payload, _ := proto.Marshal(finalResponse)

	s.pushToOutBox(ctx, qtx, transaction.TransactionID+"_FINAL", "payment.response.v1", payload)

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

	s.pushToOutBox(ctx, qtx, transaction.TransactionID+"_CREDIT", "bank.instruction.v1", payload)

}

func (s *txnsvc) handleCreditSuccess(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string) {
	qtx.UpdateTransactionStatus(ctx, repo.UpdateTransactionStatusParams{
		TransactionID:   transaction.TransactionID,
		BankReferenceID: utils.ToPGText(bankReferenceId),
		Status:          "CREDIT_SUCCESS",
	})

	finalResponse := &pb.PaymentResponse{
		TransactionId:   transaction.TransactionID,
		BankReferenceId: bankReferenceId,
		Status:          "SUCCESS",
	}

	payload, _ := proto.Marshal(finalResponse)

	s.pushToOutBox(ctx, qtx, transaction.TransactionID+"_FINAL", "payment.response.v1", payload)

}

func (s *txnsvc) pushToOutBox(ctx context.Context, qtx repo.Querier, transactionId string, topic string, payload []byte) {
	qtx.CreateOutboxEntry(ctx, repo.CreateOutboxEntryParams{
		TransactionID: transactionId,
		Topic:         topic,
		Payload:       payload,
	})
}
