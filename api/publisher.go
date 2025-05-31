// publisher.go
package api

import (
	"encoding/json"
	"fmt"
	"log"
	"rapper/utils"

	"github.com/streadway/amqp"
)

type RabbitMQPublisher struct {
	MapTopicChan map[string]*amqp.Channel
	amqpURL      string
}

func NewRabitMQPublisherService(amqpURL string) *RabbitMQPublisher {
	return &RabbitMQPublisher{
		amqpURL:      amqpURL,
		MapTopicChan: make(map[string]*amqp.Channel),
	}
}

func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func (p *RabbitMQPublisher) Publish(msg any) {
	msgTopic := utils.GetStructName(msg)

	ch := p.MapTopicChan[msgTopic]

	if ch == nil {
		fmt.Println("Creating new channel for topic:", msgTopic)
		conn, err := amqp.Dial(p.amqpURL)
		FailOnError(err, "Failed to connect to RabbitMQ")

		ch, err = conn.Channel()
		FailOnError(err, "Failed to open a channel")
		p.MapTopicChan[msgTopic] = ch

		err = ch.ExchangeDeclare(
			msgTopic, // name
			"topic",  // type
			true,     // durable
			false,    // auto-deleted
			false,    // internal
			false,    // no-wait
			nil,      // arguments
		)
		FailOnError(err, "Failed to declare an exchange")

		p.MapTopicChan[msgTopic] = ch
		fmt.Println("Channel created for topic:", msgTopic)
	}

	byteBody, err := json.Marshal(msg)
	if err != nil {
		fmt.Println("Getting Error ", err)
	}

	err = ch.Publish(
		msgTopic,
		"", // routing key
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        byteBody,
		})
	FailOnError(err, "Failed to publish a message")

}

func (p *RabbitMQPublisher) Close() {
	for topic, ch := range p.MapTopicChan {
		err := ch.Close()
		if err != nil {
			log.Printf("Failed to close channel for topic %s: %s", topic, err)
		} else {
			log.Printf("Channel for topic %s closed successfully", topic)
		}
		delete(p.MapTopicChan, topic)
	}

	if p.amqpURL != "" {
		conn, err := amqp.Dial(p.amqpURL)
		if err != nil {
			log.Printf("Failed to connect to RabbitMQ: %s", err)
			return
		}
		defer conn.Close()
		log.Println("RabbitMQ connection closed")
	}
}
