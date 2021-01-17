package main

import (
	"context"
	"log"

	"github.com/BackAged/steadyrabbit"
	"github.com/streadway/amqp"
)

func main() {
	p, err := steadyrabbit.NewPublisher(&steadyrabbit.Config{
		URL: "amqp://localhost",
	})
	if err != nil {
		log.Fatalf("unable to instantiate publisher: %s", err)
	}

	queueName := "shahin"

	if err = p.Publish(context.Background(), queueName, []byte("shahin")); err != nil {
		log.Fatalf("unable to instantiate publisher: %s", err)
	}

	c, err := steadyrabbit.NewConsumer(&steadyrabbit.Config{
		URL: "amqp://localhost",
		Consumer: &steadyrabbit.ConsumerConfig{
			QueueConfig: &steadyrabbit.QueueConfig{
				QueueName:    queueName,
				QueueDeclare: true,
				QueueDurable: true,
			},
		},
	})
	if err != nil {
		log.Fatalf("unable to instantiate consumer: %s", err)
	}

	c.Consume(context.Background(), func(msg amqp.Delivery) error {
		log.Println("got message: ", msg)
		return msg.Ack(false)
	})

}
