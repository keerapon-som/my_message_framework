package api

import (
	"encoding/json"
	"rapper/utils"
)

type HandlerInterface interface {
	TopicName() string
	Handle(msg []byte)
}

type RabbitMQMessageHandler[T any] struct {
	handlerFunc func(msg T) error
}

func AddHandler[T any](fn func(msg T) error) HandlerInterface {

	return &RabbitMQMessageHandler[T]{
		handlerFunc: fn,
	}
}

func (h *RabbitMQMessageHandler[T]) TopicName() string {
	var t T
	return utils.GetStructName(t)
}

func (h *RabbitMQMessageHandler[T]) Handle(msg []byte) {

	var t T
	json.Unmarshal(msg, &t)

	h.handlerFunc(t)
}
