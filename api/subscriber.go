// subscriber.go
package api

import (
	"github.com/streadway/amqp"
)

type RabitMQSubscriber struct {
	amqpURL string
}

func NewRabitMQSubscriberService(amqpURL string) *RabitMQSubscriber {
	return &RabitMQSubscriber{
		amqpURL: amqpURL,
	}
}

func (s *RabitMQSubscriber) Subscribe(handlers ...HandlerInterface) {

	for _, handler := range handlers {
		go func(h HandlerInterface) {
			conn, err := amqp.Dial(s.amqpURL)
			FailOnError(err, "Failed to connect to RabbitMQ")
			defer conn.Close()

			ch, err := conn.Channel()
			FailOnError(err, "Failed to open a channel")
			defer ch.Close()

			err = ch.ExchangeDeclare(
				h.TopicName(),
				"topic", // type
				true,    // durable
				false,   // auto-deleted
				false,   // internal
				false,   // no-wait
				nil,     // arguments
			)
			FailOnError(err, "Failed to declare an exchange")

			q, err := ch.QueueDeclare(
				"",    // random queue name
				false, // durable
				false, // delete when unused
				true,  // exclusive
				false, // no-wait
				nil,   // arguments
			)
			FailOnError(err, "Failed to declare a queue")

			err = ch.QueueBind(
				q.Name, // queue name
				"",     // routing key
				h.TopicName(),
				false,
				nil)
			FailOnError(err, "Failed to bind a queue")

			msgs, err := ch.Consume(
				q.Name, // queue
				"",     // consumer
				true,   // auto-ack
				true,   // exclusive
				false,  // no-local
				false,  // no-wait
				nil,    // args
			)
			FailOnError(err, "Failed to register a consumer")

			for d := range msgs {
				h.Handle(d.Body)
			}

		}(handler)

	}
}
