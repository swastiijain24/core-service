package services

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/swastiijain24/core/internals/kafka"
	pb "github.com/swastiijain24/core/internals/pb"
	repo "github.com/swastiijain24/core/internals/repositories"
	"github.com/swastiijain24/core/internals/utils"
	"google.golang.org/protobuf/proto"
)

type TransactionService interface {
	NewTransaction(ctx context.Context, transactionId string, payerAccountId string, payeeAccountId string, amount int64, payerBankCode string, payeeBankCode string) error
	ProcessBankResponse(ctx context.Context, transactionId string, bankReferenceId string, success bool, errorMessage string, txnType string) error
	handleCreditFailure(ctx context.Context, qtx repo.Querier, transaction repo.Transaction)
	handleCreditSuccess(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string)
	handleDebitFailure(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string)
	handleDebitSuccess(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string)
	pushToOutBox(ctx context.Context, qtx repo.Querier, outboxKey string, transactionId string, topic string, payload []byte) error 
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

func (s *txnsvc) NewTransaction(ctx context.Context, transactionId string, payerAccountId string, payeeAccountId string, amount int64, payerBankCode string, payeeBankCode string) error {
	dbTx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error to begin database transaction: %w", err)
	}
	defer dbTx.Rollback(ctx)

	qtx := repo.New(dbTx)

	_, err = qtx.CreateTransaction(ctx, repo.CreateTransactionParams{
		TransactionID:  transactionId,
		PayerAccountID: payerAccountId,
		PayeeAccountID: payeeAccountId,
		Amount:         amount,
		Status:         "DEBIT_PENDING",
		PayerBankCode:  utils.ToPGText(payerBankCode),
		PayeeBankCode:  utils.ToPGText(payeeBankCode),
	})
	if err != nil {
		return fmt.Errorf("error creating transaction: %w", err)
	}

	message := &pb.BankRequest{
		TransactionId:  transactionId,
		PayerAccountId: payerAccountId,
		PayeeAccountId: payeeAccountId,
		Amount:         amount,
		Type:           "DEBIT",
		BankCode:       payerBankCode,  
	}

	payload, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("error packing message: %w", err)
	}

	err = s.pushToOutBox(ctx, qtx, transactionId, transactionId, "bank.instruction.v1", payload)
	if err != nil {
		return fmt.Errorf("error pushing message to outbox: %w", err)
	}

	dbTx.Commit(ctx)

	return nil 
}

func (s *txnsvc) ProcessBankResponse(ctx context.Context, transactionId string, bankReferenceId string, success bool, errorMessage string, txnType string) error {
	dbTx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error to begin database transaction: %w", err)
	}
	defer dbTx.Rollback(ctx)

	qtx := repo.New(dbTx)

	transaction, err := qtx.GetTransaction(ctx, transactionId)
	if err != nil {
		return fmt.Errorf("error fetching transaction: %w", err)
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
			s.handleCreditFailure(ctx, qtx, transaction)
		}
	}

	dbTx.Commit(ctx)

	return nil 
}

func (s *txnsvc) handleDebitSuccess(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string) {
	qtx.UpdateDebitLeg(ctx, repo.UpdateDebitLegParams{
		TransactionID:  transaction.TransactionID,
		DebitBankRef:   utils.ToPGText(bankReferenceId),
		Status:         "DEBIT_SUCCESS",
	})

	creditReq := &pb.BankRequest{
		TransactionId:  transaction.TransactionID,
		Type:           "CREDIT",
		PayerAccountId: transaction.PayerAccountID,
		PayeeAccountId: transaction.PayeeAccountID,
		Amount:         transaction.Amount,
		BankCode:       transaction.PayeeBankCode.String, 
	}

	payload, _ := proto.Marshal(creditReq)
	s.pushToOutBox(ctx, qtx, transaction.TransactionID+"_CREDIT", transaction.TransactionID, "bank.instruction.v1", payload)
}

func (s *txnsvc) handleDebitFailure(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string) {
	qtx.UpdateDebitLeg(ctx, repo.UpdateDebitLegParams{
		TransactionID:  transaction.TransactionID,
		DebitBankRef:   utils.ToPGText(bankReferenceId),
		Status:         "DEBIT_FAILED",
	})

	finalResponse := &pb.PaymentResponse{
		TransactionId: transaction.TransactionID,
		Status:        "FAILED",
	}

	payload, _ := proto.Marshal(finalResponse)
	s.pushToOutBox(ctx, qtx, transaction.TransactionID+"_FINAL", transaction.TransactionID, "payment.response.v1", payload)
}

func (s *txnsvc) handleCreditSuccess(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string) {
	qtx.UpdateCreditLeg(ctx, repo.UpdateCreditLegParams{
		TransactionID:  transaction.TransactionID,
		CreditBankRef:  utils.ToPGText(bankReferenceId),
		Status:         "CREDIT_SUCCESS",
	})

	finalResponse := &pb.PaymentResponse{
		TransactionId: transaction.TransactionID,
		Status:        "SUCCESS",
	}

	payload, _ := proto.Marshal(finalResponse)
	s.pushToOutBox(ctx, qtx, transaction.TransactionID+"_FINAL", transaction.TransactionID, "payment.response.v1", payload)
}

func (s *txnsvc) handleCreditFailure(ctx context.Context, qtx repo.Querier, transaction repo.Transaction) {
	if transaction.RetryCount.Int32 < 3 {
		qtx.UpdateCreditLeg(ctx, repo.UpdateCreditLegParams{
			TransactionID: transaction.TransactionID,
			Status:        "CREDIT_PENDING",
		})
		qtx.IncrementRetryCount(ctx, transaction.TransactionID)

		retryReq := &pb.BankRequest{
			TransactionId:  transaction.TransactionID,
			Type:           "CREDIT",
			PayerAccountId: transaction.PayerAccountID,
			PayeeAccountId: transaction.PayeeAccountID,
			Amount:         transaction.Amount,
			BankCode:       transaction.PayeeBankCode.String,
		}

		payload, _ := proto.Marshal(retryReq)
		retryKey := fmt.Sprintf("%s_RETRY_%d", transaction.TransactionID, transaction.RetryCount.Int32+1)
		s.pushToOutBox(ctx, qtx, retryKey, transaction.TransactionID, "bank.instruction.v1", payload)
		return
	}

	qtx.UpdateCreditLeg(ctx, repo.UpdateCreditLegParams{
		TransactionID: transaction.TransactionID,
		Status:        "CREDIT_FAILED",
	})

	refundReq := &pb.BankRequest{
		TransactionId:  transaction.TransactionID,
		Type:           "CREDIT",
		PayerAccountId: "SETTLEMENT_ACCOUNT",
		PayeeAccountId: transaction.PayerAccountID,
		Amount:         transaction.Amount,
		BankCode:       transaction.PayerBankCode.String, 
	}

	payload, _ := proto.Marshal(refundReq)
	err := s.pushToOutBox(ctx, qtx, transaction.TransactionID+"_REFUND", transaction.TransactionID, "bank.instruction.v1", payload)

	if err != nil {
        qtx.UpdateCreditLeg(ctx, repo.UpdateCreditLegParams{
            TransactionID: transaction.TransactionID,
            Status:        "MANUAL_INTERVENTION_REQUIRED",
            FailureReason: utils.ToPGText("Credit failed, Refund failed to save to outbox"),
        })
        return
    }

}

func (s *txnsvc) pushToOutBox(ctx context.Context, qtx repo.Querier, outboxKey string, transactionId string, topic string, payload []byte) error {
	_, err := qtx.CreateOutboxEntry(ctx, repo.CreateOutboxEntryParams{
		OutboxKey:     outboxKey,
		TransactionID: transactionId,
		Topic:         topic,
		Payload:       payload,
	})
	if err != nil {
		return err
	}
	return nil 
}