package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

func main() {
	w := &kafka.Writer{
		Addr:                   kafka.TCP("0.0.0.0:9092"),
		Topic:                  "topic-A",
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}

	var err error
	const retries = 10
	for i := 0; i < retries; i++ {
		messages := []kafka.Message{
			{
				Key:   []byte("Key-B"),
				Value: []byte(fmt.Sprintf("massage %d", i)),
			},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// attempt to create topic prior to publishing the message
		err = w.WriteMessages(ctx, messages...)
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}

		if err != nil {
			log.Fatalf("unexpected error %v", err)
		}
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
