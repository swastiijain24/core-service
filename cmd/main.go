package main

import (
	"time"

	"github.com/swastiijain24/core/internals/kafka"
	"github.com/swastiijain24/core/internals/services"
	"github.com/swastiijain24/npci-shared/constants"
)

func main() {
	address := "localhost:9092"
	brokers := []string{address}

	producerA := kafka.NewProducer(address, constants.TopicBankInstruction)
	defer producerA.Close()
	producerB := kafka.NewProducer(address, constants.TopicPaymentResponse)
	defer producerB.Close()

	consumerA := kafka.NewConsumer(brokers, constants.TopicBankOutcome)
	defer consumerA.Close()
	consumerB := kafka.NewConsumer(brokers, constants.TopicPaymentRequest)
	defer consumerB.Close()

	service := services.NewService(producerA, producerB, consumerA, consumerB)

	time.Sleep(3 * time.Second)

	go service.ConsumeMsg()
	go service.ProduceMsg()
	go service.ConsumeMsg1()
	go service.ProduceMsg1()


	select {}
}