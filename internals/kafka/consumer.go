package kafka

import (
	"github.com/segmentio/kafka-go"
)

func NewConsumer(brokers []string , topic string ) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: "group-1",
	})
}
