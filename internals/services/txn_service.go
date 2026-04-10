package services

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

type Service struct {
	producerA *kafka.Writer
	producerB *kafka.Writer
	consumerA *kafka.Reader
	consumerB *kafka.Reader
}

func NewService(producerA *kafka.Writer, producerB *kafka.Writer, consumerA *kafka.Reader, consumerB *kafka.Reader) *Service {
	return &Service{
		producerA: producerA,
		producerB: producerB,
		consumerA: consumerA,
		consumerB: consumerB,
	}
}

func (s *Service) ProduceMsg() {
	msg := kafka.Message{
		Key:   []byte("key1"),
		Value: []byte("order to bank!"),
	}

	err := s.producerA.WriteMessages(context.Background(), msg)
	if err != nil {
		log.Fatal("failed to write message:", err)
	}

	// log.Println("Message sent successfully")

}

func (s *Service) ProduceMsg1() {
	msg := kafka.Message{
		Key:   []byte("key1"),
		Value: []byte("txn response sent from core!"),
	}

	err := s.producerB.WriteMessages(context.Background(), msg)
	if err != nil {
		log.Fatal("failed to write message:", err)
	}

	// log.Println("Message sent successfully")

}

func (s *Service) ConsumeMsg() {
	ctx := context.Background()
	for {
		msg, err := s.consumerA.FetchMessage(ctx)
		if err != nil {
			log.Fatal("error reading message:", err)
		}

		fmt.Printf("Message: %s\n", string(msg.Value))

		err = s.consumerA.CommitMessages(ctx, msg)
		if err != nil {
			log.Fatal("commit failed:", err)
		}
	}
}


func (s *Service) ConsumeMsg1() {
	ctx:= context.Background()
	for {
		msg, err := s.consumerB.ReadMessage(ctx)
		if err != nil {
			log.Fatal("error reading message:", err)
		}

		fmt.Printf("Message: %s\n", string(msg.Value))

		err = s.consumerB.CommitMessages(ctx, msg)
		if err != nil {
			log.Fatal("commit failed:", err)
		}
	}
}
