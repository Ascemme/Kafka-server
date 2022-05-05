package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

func main() {
	go listenerTow(2)
	listenerOne(1)

}

func listenerOne(id int) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"0.0.0.0:9092"},
		Topic:          "topic-A",
		Partition:      0,
		GroupID:        "1",
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		IsolationLevel: 1,
	})
	defer r.Close()
	for {
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			fmt.Println("commit 1", err)
			break
		}

		err = r.CommitMessages(context.Background(), m)
		if err != nil {
			fmt.Println("commit 2", err)

		}
		if m.Time.After(time.Now().Add(time.Second)) {
			fmt.Println("time 2", err)
			break
		}
		fmt.Printf("message at offset %d: %s = %s  gorutin %d \n", m.Offset, string(m.Key), string(m.Value), id)
	}
}
func listenerTow(id int) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"0.0.0.0:9092"},
		Topic:          "topic-A",
		Partition:      0,
		GroupID:        "2",
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		IsolationLevel: 0,
	})
	defer r.Close()
	for {
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			fmt.Println("commit 1", err)
			break
		}

		err = r.CommitMessages(context.Background(), m)
		if err != nil {
			fmt.Println("commit 2", err)
		}

		if m.Time.After(time.Now().Add(time.Second)) {
			fmt.Println("time 2", err)
			break
		}
		fmt.Printf("message at offset %d: %s = %s  gorutin %d \n", m.Offset, string(m.Key), string(m.Value), id)
	}
}
