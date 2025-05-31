package mymessage

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type MyMessageKafkaConsumer struct {
	Brokers []string
	GroupID string
}

func MyMessageKafkaConsumerService(Brokers []string, GroupID string) *MyMessageKafkaConsumer {
	return &MyMessageKafkaConsumer{
		Brokers: Brokers,
		GroupID: GroupID,
	}
}

func (m *MyMessageKafkaConsumer) AddHandler(h MessageHandlerInterface) {

	fmt.Println("AddHandler Topic ", h.Topic())
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: m.Brokers,
		Topic:   h.Topic(),
		GroupID: m.GroupID,
	})

	go func() {

		for {
			msg, err := r.ReadMessage(context.Background())

			if err != nil {
				panic(err)
			}
			h.Do(msg.Value)
		}
	}()

}
