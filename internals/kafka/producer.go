package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(address string) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(address),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  5,
		BatchSize: 1,
		Async:     false,
	}
	return &Producer{
		writer: writer,
	}
}

func (p *Producer) ProduceEvent(ctx context.Context, key string, value []byte, topic string) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
		Topic: topic,
	})
}
