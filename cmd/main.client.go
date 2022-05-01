package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

func main() {

	go listenerOne(1)
	go listenerThird(3)
	listenerTow(2)
}

func listenerOne(id int) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"0.0.0.0:9092"},
		Topic:     "topic-A",
		GroupID:   "1",
		Partition: 0,
		MinBytes:  1e3,
		MaxBytes:  1e9,
	})

	r.SetOffsetAt(context.Background(), time.Now())

	for {
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			fmt.Println("reading 1", err)
			break
		}
		err = r.CommitMessages(context.Background(), m)
		if err != nil {
			fmt.Println("commit 1", err)
			break
		}
		if m.Time.After(time.Now().Add(time.Second)) {
			fmt.Println("time 1", err)
			break
		}

		fmt.Printf("message at offset %d: %s = %s   gorutin %d \n", m.Offset, string(m.Key), string(m.Value), id)
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}

func listenerTow(id int) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"0.0.0.0:9092"},
		Topic:     "topic-A",
		GroupID:   "2",
		Partition: 0,
		MinBytes:  1e3,
		MaxBytes:  1e9,
	})
	for {
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			fmt.Println("reading 2", err)
			break
		}
		err = r.CommitMessages(context.Background(), m)
		if err != nil {
			fmt.Println("commit 1", err)
			break
		}

		if m.Time.After(time.Now().Add(time.Second)) {
			fmt.Println("time 2", err)
			break
		}

		fmt.Printf("message at offset %d: %s = %s  gorutin %d \n", m.Offset, string(m.Key), string(m.Value), id)
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}

func listenerThird(id int) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"0.0.0.0:9092"},
		Topic:     "topic-A",
		GroupID:   "3",
		Partition: 0,
		MinBytes:  1e3,
		MaxBytes:  1e9,
	})
	for {
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			fmt.Println("reading 2", err)
			break
		}

		if m.Time.After(time.Now().Add(time.Second)) {
			fmt.Println("time 2", err)
			break
		}

		fmt.Printf("message at offset %d: %s = %s  gorutin %d \n", m.Offset, string(m.Key), string(m.Value), id)
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
