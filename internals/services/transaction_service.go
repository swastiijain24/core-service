package services

import (
	"context"
	"fmt"
	"log"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/swastiijain24/core/internals/kafka"
	pb "github.com/swastiijain24/core/internals/pb"
	repo "github.com/swastiijain24/core/internals/repositories"
	"github.com/swastiijain24/core/internals/utils"
	"google.golang.org/protobuf/proto"
)

type TransactionService interface {
	NewTransaction(ctx context.Context, transactionId string, payerAccountId string, payeeAccountId string, amount int64, payerBankCode string, payeeBankCode string, mpin string) error
	ProcessBankResponse(ctx context.Context, transactionId string, bankReferenceId string, success bool, errorMessage string, txnType string) error
	handleCreditFailure(ctx context.Context, qtx repo.Querier, transaction repo.Transaction) error
	handleCreditSuccess(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string) error
	handleDebitFailure(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string) error
	handleDebitSuccess(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string) error
	GetStuckTransactions(ctx context.Context) ([]repo.Transaction, error)
	GetTransactionById(ctx context.Context, transactionId string) (repo.Transaction, error)
	UpdateTransactionStatus(ctx context.Context, transactionId string, status string) error
}

type txnsvc struct {
	db            *pgxpool.Pool
	repo          repo.Querier
	bankProducer  *kafka.Producer
	outboxService OutboxService
}

func NewTransactionService(repo repo.Querier, db *pgxpool.Pool, bankProducer *kafka.Producer, outboxService OutboxService) TransactionService {
	return &txnsvc{
		db:            db,
		repo:          repo,
		bankProducer:  bankProducer,
		outboxService: outboxService,
	}
}

func (s *txnsvc) NewTransaction(ctx context.Context, transactionId string, payerAccountId string, payeeAccountId string, amount int64, payerBankCode string, payeeBankCode string, mpin string) error {
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
		BankCode:       payerBankCode,
		Operation: &pb.BankRequest_Debit{
			Debit: &pb.DebitDetails{
				Mpin: mpin,
			},
		},
	}

	payload, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("error packing message: %w", err)
	}

	err = s.outboxService.PushToOutbox(ctx, qtx, transactionId, transactionId, "bank.instruction.v1", payload)
	if err != nil {
		return fmt.Errorf("error pushing message to outbox: %w", err)
	}
	log.Print("pushed bank request to outbox")
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

	var processErr error

	switch txnType {
	case "DEBIT":
		if success {
			processErr = s.handleDebitSuccess(ctx, qtx, transaction, bankReferenceId)
		} else {
			processErr = s.handleDebitFailure(ctx, qtx, transaction, bankReferenceId)
		}
	case "CREDIT":
		if transaction.Status == "REFUNDING" {
			if success {
				processErr = s.handleRefundSuccess(ctx, qtx, transaction)
			} else {
				processErr = s.handleRefundFailure(ctx, qtx, transaction)
			}
		} else {
			if success {
				processErr = s.handleCreditSuccess(ctx, qtx, transaction, bankReferenceId)
			} else {
				processErr = s.handleCreditFailure(ctx, qtx, transaction)
			}
		}
	}
	if processErr != nil {
		return fmt.Errorf("%s", processErr)
	}

	dbTx.Commit(ctx)

	log.Print("handled the bank response")
	return nil
}

func (s *txnsvc) handleDebitSuccess(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string) error {
	_, err := qtx.UpdateDebitLeg(ctx, repo.UpdateDebitLegParams{
		TransactionID: transaction.TransactionID,
		DebitBankRef:  utils.ToPGText(bankReferenceId),
		Status:        "DEBIT_SUCCESS",
	})
	if err != nil {
		return err
	}

	creditReq := &pb.BankRequest{
		TransactionId:  transaction.TransactionID,
		PayerAccountId: transaction.PayerAccountID,
		PayeeAccountId: transaction.PayeeAccountID,
		Amount:         transaction.Amount,
		BankCode:       transaction.PayeeBankCode.String,
		Operation:      &pb.BankRequest_Credit{},
	}

	payload, err := proto.Marshal(creditReq)
	if err != nil {
		return err
	}
	return s.outboxService.PushToOutbox(ctx, qtx, transaction.TransactionID+"_CREDIT", transaction.TransactionID, "bank.instruction.v1", payload)
}

func (s *txnsvc) handleDebitFailure(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string) error {
	_, err := qtx.UpdateDebitLeg(ctx, repo.UpdateDebitLegParams{
		TransactionID: transaction.TransactionID,
		DebitBankRef:  utils.ToPGText(bankReferenceId),
		Status:        "DEBIT_FAILED",
	})
	if err != nil {
		return err
	}

	finalResponse := &pb.PaymentResponse{
		TransactionId: transaction.TransactionID,
		Status:        "FAILED",
	}

	payload, err := proto.Marshal(finalResponse)
	if err != nil {
		return err
	}
	return s.outboxService.PushToOutbox(ctx, qtx, transaction.TransactionID+"_FINAL", transaction.TransactionID, "payment.response.v1", payload)
}

func (s *txnsvc) handleCreditSuccess(ctx context.Context, qtx repo.Querier, transaction repo.Transaction, bankReferenceId string) error {
	_, err := qtx.UpdateCreditLeg(ctx, repo.UpdateCreditLegParams{
		TransactionID: transaction.TransactionID,
		CreditBankRef: utils.ToPGText(bankReferenceId),
		Status:        "CREDIT_SUCCESS",
	})
	if err != nil {
		return err
	}

	finalResponse := &pb.PaymentResponse{
		TransactionId: transaction.TransactionID,
		Status:        "SUCCESS",
	}

	payload, err := proto.Marshal(finalResponse)
	if err != nil {
		return err
	}
	return s.outboxService.PushToOutbox(ctx, qtx, transaction.TransactionID+"_FINAL", transaction.TransactionID, "payment.response.v1", payload)
}

func (s *txnsvc) handleCreditFailure(ctx context.Context, qtx repo.Querier, transaction repo.Transaction) error {
	if transaction.RetryCount.Int32 < 3 {
		_, err := qtx.UpdateCreditLeg(ctx, repo.UpdateCreditLegParams{
			TransactionID: transaction.TransactionID,
			Status:        "CREDIT_PENDING",
		})
		if err != nil {
			return err
		}
		err = qtx.IncrementRetryCount(ctx, transaction.TransactionID)

		if err != nil {
			return err
		}

		retryReq := &pb.BankRequest{
			TransactionId:  transaction.TransactionID,
			PayerAccountId: transaction.PayerAccountID,
			PayeeAccountId: transaction.PayeeAccountID,
			Amount:         transaction.Amount,
			BankCode:       transaction.PayeeBankCode.String,
			Operation:      &pb.BankRequest_Credit{},
		}

		payload, err := proto.Marshal(retryReq)
		if err != nil {
			return err
		}
		retryKey := fmt.Sprintf("%s_RETRY_%d", transaction.TransactionID, transaction.RetryCount.Int32+1)
		return s.outboxService.PushToOutbox(ctx, qtx, retryKey, transaction.TransactionID, "bank.instruction.v1", payload)

	}

	qtx.UpdateCreditLeg(ctx, repo.UpdateCreditLegParams{
		TransactionID: transaction.TransactionID,
		Status:        "REFUNDING",
	})

	refundReq := &pb.BankRequest{
		TransactionId:  transaction.TransactionID,
		PayerAccountId: "SETTLEMENT_ACCOUNT",
		PayeeAccountId: transaction.PayerAccountID,
		Amount:         transaction.Amount,
		BankCode:       transaction.PayerBankCode.String,
		Operation:      &pb.BankRequest_Credit{},
	}

	payload, err := proto.Marshal(refundReq)
	if err != nil {
		return err
	}
	return s.outboxService.PushToOutbox(ctx, qtx, transaction.TransactionID+"_REFUND", transaction.TransactionID, "bank.instruction.v1", payload)

}

func (s *txnsvc) handleRefundSuccess(ctx context.Context, qtx repo.Querier, transaction repo.Transaction) error {
	_, err := qtx.UpdateDebitLeg(ctx, repo.UpdateDebitLegParams{
		TransactionID: transaction.TransactionID,
		Status:        "REFUNDED",
	})
	if err != nil {
		return err
	}

	finalResponse := &pb.PaymentResponse{
		TransactionId: transaction.TransactionID,
		Status:        "FAILED",
	}

	payload, err := proto.Marshal(finalResponse)
	if err != nil {
		return err
	}
	return s.outboxService.PushToOutbox(ctx, qtx, transaction.TransactionID+"_REFUNDED", transaction.TransactionID, "payment.response.v1", payload)
}

func (s *txnsvc) handleRefundFailure(ctx context.Context, qtx repo.Querier, transaction repo.Transaction) error {

	_, err := qtx.UpdateDebitLeg(ctx, repo.UpdateDebitLegParams{
		TransactionID: transaction.TransactionID,
		Status:        "MANUAL_INTERVENTION_REQUIRED",
		FailureReason: utils.ToPGText("Credit failed, Refund failed to save to outbox"),
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *txnsvc) GetStuckTransactions(ctx context.Context) ([]repo.Transaction, error) {
	return s.repo.GetStuckTransactions(ctx)
}

func (s *txnsvc) GetTransactionById(ctx context.Context, transactionId string) (repo.Transaction, error) {
	return s.repo.GetTransaction(ctx, transactionId)
}

func (s *txnsvc) UpdateTransactionStatus(ctx context.Context, transactionId string, status string) error {
	return s.repo.UpdateTransactionStatus(ctx, repo.UpdateTransactionStatusParams{
		TransactionID: transactionId,
		Status: status,
	})
}
