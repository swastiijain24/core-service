package main

import (
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	// "github.com/swastiijain24/core/internals/kafka"
)

func main() {

	ctx := context.Background()
	dsn := os.Getenv("GOOSE_DBSTRING")

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	log.Printf("connected to database")

	// brokers:= []string {"localhost:9092"}

	// reqConsumer := kafka.NewConsumer(brokers, "payment.request.v1")

	// select {}
}
