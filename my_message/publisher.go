package mymessage

import (
	"context"
	"encoding/json"
	"fmt"
	"mymessage/utils"
	"time"

	"github.com/segmentio/kafka-go"
)

type MyMessageKafkaPublisherService struct {
	writer *kafka.Writer
	ctx    context.Context
}

func MyMessageKafkaPublisher(ctx context.Context, network string, address string) *MyMessageKafkaPublisherService {

	return &MyMessageKafkaPublisherService{
		ctx: ctx,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(address),
			Balancer:     &kafka.LeastBytes{},
			BatchSize:    100,                   // Batch up to 100 messages
			BatchBytes:   1048576,               // 1MB per batch
			BatchTimeout: 10 * time.Millisecond, // Wait up to 10ms for batch to fill
			Async:        true,                  // Non-blocking async writes
		},
	}
}
func (p *MyMessageKafkaPublisherService) Publish(msg any) {
	topic := utils.GetStructName(msg)
	fmt.Println("pub topic ", topic)

	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		fmt.Printf("Error marshaling message: %v\n", err)
		return
	}

	// Use a goroutine to prevent blocking
	go func() {
		err := p.writer.WriteMessages(p.ctx, kafka.Message{
			Topic: topic,
			Value: jsonMsg,
		})
		if err != nil {
			fmt.Printf("Error writing message: %v\n", err)
		}
	}()
}

// Close should be called when the publisher is no longer needed
func (p *MyMessageKafkaPublisherService) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}
