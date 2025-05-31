package main

import (
	"fmt"
	"rapper/api"
	"time"
)

func main() {

	amq := "amqp://guest:guest@localhost:5672/"

	pub := api.NewRabitMQPublisherService(amq)

	go func() {
		i := 1
		for i < 10 {
			pub.Publish(EIEI{
				A: fmt.Sprintf("yim%d", i),
				B: fmt.Sprintf("eieizzz%d", i),
			})

			pub.Publish(Yimmy{
				Tim: i,
			})
			// Simulate some processing time
			i++
			time.Sleep(1 * time.Second)
			fmt.Println("Pub")
		}
	}()

	sub := api.NewRabitMQSubscriberService(amq)

	sub.Subscribe(
		api.AddHandler(YEAH),
		api.AddHandler(TIMME2),
	)

	select {}

}

type EIEI struct {
	A string
	B string
}

type Yimmy struct {
	Tim int
}

func YEAH(msg EIEI) error {
	fmt.Println(msg)
	return nil
}

func TIMME2(msg Yimmy) error {
	fmt.Println(msg)
	return nil
}
