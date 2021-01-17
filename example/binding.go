package main

import (
	"context"
	"log"

	"github.com/BackAged/steadyrabbit"
	"github.com/streadway/amqp"
)

func main() {
	topic := "shahin"
	exchangeName := "some.exchange"
	exchangeType := amqp.ExchangeTopic

	p, err := steadyrabbit.NewPublisher(&steadyrabbit.Config{
		URL: "amqp://localhost",
		Publisher: &steadyrabbit.PublisherConfig{
			Exchange: &steadyrabbit.ExchangeConfig{
				ExchangeName:    exchangeName,
				ExchangeType:    exchangeType,
				ExchangeDeclare: true,
			},
		},
	})
	if err != nil {
		log.Fatalf("unable to instantiate publisher: %s", err)
	}

	if err = p.Publish(context.Background(), topic, []byte("shahin")); err != nil {
		log.Fatalf("unable to instantiate publisher: %s", err)
	}

	c, err := steadyrabbit.NewConsumer(&steadyrabbit.Config{
		URL: "amqp://localhost",
		Consumer: &steadyrabbit.ConsumerConfig{
			QueueConfig: &steadyrabbit.QueueConfig{
				QueueName:    "some.queue",
				QueueDeclare: true,
				QueueDurable: true,
			},
			Bindings: []*steadyrabbit.BindingConfig{
				&steadyrabbit.BindingConfig{
					Exchange: &steadyrabbit.ExchangeConfig{
						ExchangeName:    exchangeName,
						ExchangeDeclare: true,
						ExchangeType:    exchangeType,
					},
					RoutingKeys: []string{topic},
				},
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
