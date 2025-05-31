package mymessage

import (
	"context"
	"encoding/json"
	"mymessage/utils"

	"github.com/segmentio/kafka-go"
)

type MyMessageKafkaPublisherService struct {
	ctx     context.Context
	network string
	address string
}

func MyMessageKafkaPublisher(ctx context.Context, network string, address string) *MyMessageKafkaPublisherService {
	return &MyMessageKafkaPublisherService{
		ctx:     ctx,
		network: network,
		address: address,
	}
}

func (p *MyMessageKafkaPublisherService) Publish(msg any) {
	topic := utils.GetStructName(msg)
	partition := 10

	conn, err := kafka.DialLeader(context.Background(), p.network, p.address, topic, partition)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	byteMsg, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	_, err = conn.WriteMessages(
		kafka.Message{Value: byteMsg},
	)

	if err != nil {
		panic(err)
	}

}

// func LoopPub() {
// 	fmt.Println("Start Loop pub")
// 	topic := "MyTopic1"
// 	partition := 0

// 	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
// 	if err != nil {
// 		panic(err)
// 	}
// 	// defer conn.Close()

// 	for i := 0; i < 100; i++ {

// 		mes := entities.MyTopic1{
// 			Yippe1: "Yippe1",
// 			Eiei:   1,
// 		}

// 		byteMsg, err := json.Marshal(mes)
// 		if err != nil {
// 			panic(err)
// 		}
// 		_, err = conn.WriteMessages(
// 			kafka.Message{Value: byteMsg},
// 		)

// 		fmt.Println("pub ", i)

// 		if err != nil {
// 			panic(err)
// 		}

// 		time.Sleep(1 * time.Second)
// 	}
// }
