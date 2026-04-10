package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

func NewProducer(address string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(address),
		Topic:    topic,
		Balancer: &kafka.Hash{},

		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
}
