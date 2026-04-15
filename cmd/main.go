package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/swastiijain24/core/internals/kafka"
	repo "github.com/swastiijain24/core/internals/repositories"
	"github.com/swastiijain24/core/internals/services"
	"github.com/swastiijain24/core/internals/workers"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found")
	}

	dsn := os.Getenv("GOOSE_DBSTRING")

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	log.Printf("connected to database")

	repo := repo.New(pool)
	kafkaAddr := os.Getenv("KAFKA_ADDR")
	Producer := kafka.NewProducer(kafkaAddr)

	txnService := services.NewTransactionService(repo, pool, Producer)
	outboxService := services.NewOutboxService(repo)

	bankConsumer := kafka.NewConsumer([]string{kafkaAddr}, "bank.response.v1" , "core-grp-1")
	bankProducer := kafka.NewProducer(kafkaAddr)
	paymentConsumer := kafka.NewConsumer([]string{kafkaAddr}, "payment.request.v1", "core-grp-2")

	defer bankConsumer.Reader.Close()
	defer paymentConsumer.Reader.Close()

	dlqProducer := kafka.NewProducer(kafkaAddr)
	paymentWorker := workers.NewPaymentWorker(paymentConsumer,dlqProducer, txnService)
	bankWorker := workers.NewBankWorker(bankConsumer,bankProducer, txnService)
	relayWorker := workers.NewRelayWorker(outboxService, Producer)

	reconProducer := kafka.NewProducer(kafkaAddr)
	reconWorker := workers.NewReconWorker(txnService, reconProducer)

	go paymentWorker.StartConsumingPaymentRequest(ctx)
	go bankWorker.StartConsumingBankResponse(ctx)
	go relayWorker.StartRelayingOutboxEntries(ctx)
	go reconWorker.StartWorker(ctx)

	log.Println("Core Service is running...")

	<-ctx.Done()

	log.Println("Shutting down...")

}
