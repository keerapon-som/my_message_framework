package mymessage

import (
	"encoding/json"
	"mymessage/utils"
)

type MessageHandlerInterface interface {
	HandlerName() string
	Topic() string
	Do(msgValue []byte)
}

type MessageHandler[T any] struct {
	Name    string
	Handler func(topicService T) error
}

func NewMessageHandler[T any](name string, handler func(topicService T) error) MessageHandlerInterface {
	return &MessageHandler[T]{
		Name:    name,
		Handler: handler,
	}
}

func (h *MessageHandler[T]) HandlerName() string {
	return h.Name
}

func (h *MessageHandler[T]) Topic() string {
	var t T
	return utils.GetStructName(t)
}

func (h *MessageHandler[T]) Do(msgValue []byte) {

	var receiveStruct T
	err := json.Unmarshal(msgValue, &receiveStruct)
	if err != nil {
		panic(err)
	}

	h.Handler(receiveStruct)

}
