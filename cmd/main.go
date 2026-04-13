package main

import (
	"context"
	"log"
	"os"
	"github.com/joho/godotenv"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/swastiijain24/core/internals/kafka"
	repo "github.com/swastiijain24/core/internals/repositories"
	"github.com/swastiijain24/core/internals/services"
	"github.com/swastiijain24/core/internals/workers"
)

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found")
	}

	ctx := context.Background()
	dsn := os.Getenv("GOOSE_DBSTRING")

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	log.Printf("connected to database")

	repo := repo.New(pool)
	Producer := kafka.NewProducer("localhost:9092")

	txnService := services.NewTransactionService(repo, pool, Producer)

	bankConsumer := kafka.NewConsumer([]string {"localhost:9092"}, "bank.response.v1")
	paymentConsumer := kafka.NewConsumer([]string {"localhost:9092"}, "payment.request.v1")

	paymentWorker := workers.NewPaymentWorker(paymentConsumer, txnService)
	bankWorker := workers.NewBankWorker(bankConsumer, txnService)
	relayWorker := workers.NewRelayWorker(repo , Producer)

	go paymentWorker.StartConsumingPaymentRequest(ctx)
	go bankWorker.StartConsumingBankResponse(ctx)
	go relayWorker.StartRelayingOutboxEntries(ctx)

	log.Println("Core Service is running...")
	<-ctx.Done() 
	log.Println("Shutting down Core Service...")

	select {}
}
