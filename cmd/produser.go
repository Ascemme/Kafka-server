package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "0.0.0.0:9092", "transactional.id": "1"})
	if err != nil {
		panic(err)
	}
	defer p.Close()
	go sender(p)

	err = p.InitTransactions(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}

	err = p.BeginTransaction()
	if err != nil {
		fmt.Println("tit")
		fmt.Println(err)
		return
	}

	topic := "topic-A"

	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte("1"),
	}, nil)

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}

func sender(p *kafka.Producer) {
	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
			} else {
				fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
			}
		}
	}
}
