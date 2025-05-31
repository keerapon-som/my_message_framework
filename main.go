package main

import (
	"context"
	"fmt"
	"mymessage/entities"
	mymessage "mymessage/my_message"
	"time"
)

func main() {

	kafkaURL := "localhost:9092"

	publisher := mymessage.MyMessageKafkaPublisher(context.Background(), "tcp", kafkaURL)

	myConsumer := mymessage.MyMessageKafkaConsumerService([]string{kafkaURL}, "group1")

	myConsumer.AddHandler(
		mymessage.NewMessageHandler("abc", HelloCQRSCommand),
	)

	myConsumer.AddHandler(
		mymessage.NewMessageHandler("yimz", Yim2),
	)

	go func() {
		i := 1
		for i < 5 {
			time.Sleep(1 * time.Second)
			i++
			publisher.Publish(TheYImZ{
				YIM: "eiei YIM",
			})
			publisher.Publish(entities.MyTopic1{
				Yippe1: "YIPPY ",
				Eiei:   i,
			})
		}
	}()

	select {}

}

type TheYImZ struct {
	YIM string
}

func Yim2(c TheYImZ) error {
	fmt.Println(c.YIM)
	return nil
}

func HelloCQRSCommand(command entities.MyTopic1) error {

	fmt.Println("HelloCQRSCommand")

	fmt.Println(command.Yippe1)
	fmt.Println(command.Eiei)

	return nil

}
