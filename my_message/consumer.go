package mymessage

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type MyMessageKafkaConsumer struct {
	Brokers     []string
	GroupID     string
	readers     []*kafka.Reader
	shutdown    chan struct{}
	wg          sync.WaitGroup
	mu          sync.Mutex
	initialized bool
}

func MyMessageKafkaConsumerService(Brokers []string, GroupID string) *MyMessageKafkaConsumer {
	return &MyMessageKafkaConsumer{
		Brokers:  Brokers,
		GroupID:  GroupID,
		readers:  make([]*kafka.Reader, 0),
		shutdown: make(chan struct{}),
	}
}

func (m *MyMessageKafkaConsumer) AddHandler(h MessageHandlerInterface) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Printf("Adding handler for topic: %s\n", h.Topic())

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        m.Brokers,
		Topic:          h.Topic(),
		GroupID:        m.GroupID,
		MinBytes:       10e3,        // 10KB
		MaxBytes:       10e6,        // 10MB
		MaxWait:        time.Second, // Maximum time to wait for new data
		CommitInterval: time.Second, // Flush commits every second
		StartOffset:    kafka.LastOffset,
	})

	m.readers = append(m.readers, reader)

	m.wg.Add(1)
	go func(reader *kafka.Reader, handler MessageHandlerInterface) {
		defer m.wg.Done()

		for {
			select {
			case <-m.shutdown:
				return
			default:
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				msg, err := reader.ReadMessage(ctx)
				cancel()

				if err != nil {
					if err == context.DeadlineExceeded {
						continue
					}
					log.Printf("Error reading message: %v", err)
					// Add a small delay to prevent tight loop on errors
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// Process the message
				handler.Do(msg.Value)
			}
		}
	}(reader, h)
}

// StartConsumers starts all registered consumers
func (m *MyMessageKafkaConsumer) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.initialized {
		return
	}
	m.initialized = true

	// Handle graceful shutdown
	go func() {
		sigterm := make(chan os.Signal, 1)
		signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
		<-sigterm
		log.Println("Shutdown signal received, closing consumers...")
		m.Close()
	}()
}

// Close gracefully shuts down all consumers
func (m *MyMessageKafkaConsumer) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Signal all consumers to stop
	close(m.shutdown)

	// Wait for all consumers to finish processing
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	// Close all readers after consumers have stopped
	select {
	case <-done:
		// All consumers have stopped
	case <-time.After(30 * time.Second):
		log.Println("Warning: Timed out waiting for consumers to stop")
	}

	// Close all readers
	for _, reader := range m.readers {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing reader: %v", err)
		}
	}

	log.Println("All consumers stopped")
}
