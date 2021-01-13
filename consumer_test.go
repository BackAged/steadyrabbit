package steadyrabbit_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/streadway/amqp"

	"github.com/BackAged/steadyrabbit"
)

var _ = Describe("Consumer", func() {
	var (
		cnf *steadyrabbit.Config
		// c   *steadyrabbit.Consumer
		// err error
	)

	JustBeforeEach(func() {
		cnf = formPublisherConfig()
		// p, err = steadyrabbit.NewPublisher(cnf)

		// Expect(err).ToNot(HaveOccurred())
		// Expect(p).ToNot(BeNil())
	})
	Describe("NewConsumer", func() {
		Context("instantiating", func() {
			It("should success with valid config", func() {
				c, err := steadyrabbit.NewConsumer(cnf)

				Expect(err).ToNot(HaveOccurred())
				Expect(c).ToNot(BeNil())
			})

			It("should success with queue config", func() {
				cnf.Consumer = &steadyrabbit.ConsumerConfig{
					QueueConfig: &steadyrabbit.QueueConfig{
						QueueName:    "something.service",
						QueueDeclare: true,
						QueueDurable: true,
					},
				}
				c, err := steadyrabbit.NewConsumer(cnf)

				Expect(err).ToNot(HaveOccurred())
				Expect(c).ToNot(BeNil())
			})

			It("should error with missing config", func() {
				c, err := steadyrabbit.NewConsumer(nil)

				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(ContainSubstring("Config can not be nil"))
				Expect(c).To(BeNil())
			})

			It("should error with unreachable rabbit server", func() {
				cnf.URL = "amqp://bad-url"

				c, err := steadyrabbit.NewConsumer(cnf)

				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(ContainSubstring("unable to dial server"))
				Expect(c).To(BeNil())
			})

			// It("should instantiates various internals", func() {
			// 	c, err := steadyrabbit.NewConsumer(cnf)

			// 	Expect(err).To(BeNil())
			// 	Expect(c).ToNot(BeNil())

			// 	Expect(c.Conn).ToNot(BeNil())
			// 	Expect(c.NotifyCloseChan).ToNot(BeNil())
			// 	Expect(c.DeliveryStream).ToNot(BeNil())
			// 	Expect(c.ConsumerChannel).ToNot(BeNil())
			// })

			// 	It("should start NotifyCloseChan watcher", func() {
			// 		c, err := steadyrabbit.NewConsumer(cnf)

			// 		Expect(err).To(BeNil())
			// 		Expect(c).ToNot(BeNil())

			// 		// Before we write errors to the notify channel, copy previous
			// 		// conn and channels so we can compare them after reconnect
			// 		oldConn := c.Conn
			// 		oldNotifyCloseChan := c.NotifyCloseChan
			// 		oldConsumerChan := c.ConsumerChannel
			// 		oldDeliveryStream := c.DeliveryStream

			// 		// Write an error to the NotifyCloseChan
			// 		c.NotifyCloseChan <- &amqp.Error{
			// 			Code:    0,
			// 			Reason:  "Test failure",
			// 			Server:  false,
			// 			Recover: false,
			// 		}

			// 		time.Sleep(1 * time.Second)

			// 		// We should've reconnected and got a new conn
			// 		Expect(c.Conn).ToNot(BeNil())
			// 		Expect(oldConn).ToNot(Equal(c.Conn))

			// 		// We should also get new channels
			// 		Expect(c.NotifyCloseChan).ToNot(BeNil())
			// 		Expect(oldNotifyCloseChan).ToNot(Equal(c.NotifyCloseChan))

			// 		Expect(c.ConsumerChannel).ToNot(BeNil())
			// 		Expect(oldConsumerChan).ToNot(Equal(c.ConsumerChannel))

			// 		Expect(c.DeliveryStream).ToNot(BeNil())
			// 		Expect(oldDeliveryStream).ToNot(Equal(c.DeliveryStream))
			// 	})
			// })

		})
	})

	Describe("Consume", func() {
		Context("consuming message with no binding", func() {
			It("should receive messages", func() {
				qName := "something.else.service"
				receivedMessages := make([]amqp.Delivery, 0)

				cnf.Consumer = &steadyrabbit.ConsumerConfig{
					QueueConfig: &steadyrabbit.QueueConfig{
						QueueName:    qName,
						QueueDeclare: true,
						QueueDurable: true,
					},
				}

				c, err := steadyrabbit.NewConsumer(cnf)
				Expect(err).ToNot(HaveOccurred())
				Expect(c).ToNot(BeNil())

				p, err := steadyrabbit.NewPublisher(cnf)
				Expect(err).ToNot(HaveOccurred())

				data := [][]byte{[]byte("shahin"), []byte("shahin"), []byte("shahin"), []byte("shahin")}
				for _, d := range data {
					err = p.Publish(context.Background(), qName, d)
					Expect(err).ToNot(HaveOccurred())
				}

				go func() {
					c.Consume(context.Background(), func(msg amqp.Delivery) error {
						receivedMessages = append(receivedMessages, msg)
						return msg.Ack(false)
					})
				}()

				// wait for to receive all the messages
				time.Sleep(1 * time.Second)

				dataReceived := [][]byte{}
				for _, msg := range receivedMessages {
					fmt.Println(msg.Exchange)
					Expect(msg.Exchange).To(Equal(p.Config.Publisher.Exchange.ExchangeName))
					Expect(msg.RoutingKey).To(Equal(qName))
					Expect(msg.ConsumerTag).To(Equal(c.Config.Consumer.ConsumerTag))

					dataReceived = append(dataReceived, msg.Body)
				}

				Expect(dataReceived).To(Equal(data))
			})
		})

		Context("consuming message with binding", func() {
			It("should receive messages with binding", func() {
				qName := "something.binding.service"
				eName := "something.binding.exchange"
				tName := "something.created"
				receivedMessages := make([]amqp.Delivery, 0)

				cnf.Consumer = &steadyrabbit.ConsumerConfig{
					QueueConfig: &steadyrabbit.QueueConfig{
						QueueName:    qName,
						QueueDeclare: true,
						QueueDurable: true,
					},
					Bindings: []*steadyrabbit.BindingConfig{
						&steadyrabbit.BindingConfig{
							Exchange: &steadyrabbit.ExchangeConfig{
								ExchangeName:    eName,
								ExchangeDeclare: true,
								ExchangeType:    amqp.ExchangeTopic,
							},
							RoutingKeys: []string{tName},
						},
					},
				}

				c, err := steadyrabbit.NewConsumer(cnf)
				Expect(err).ToNot(HaveOccurred())
				Expect(c).ToNot(BeNil())

				cnf.Publisher = &steadyrabbit.PublisherConfig{
					Exchange: &steadyrabbit.ExchangeConfig{
						ExchangeName:    eName,
						ExchangeDeclare: true,
						ExchangeType:    amqp.ExchangeTopic,
					},
				}
				p, err := steadyrabbit.NewPublisher(cnf)
				Expect(err).ToNot(HaveOccurred())

				data := [][]byte{[]byte("shahin"), []byte("shahin"), []byte("shahin"), []byte("shahin")}
				for _, d := range data {
					err = p.Publish(context.Background(), tName, d)
					Expect(err).ToNot(HaveOccurred())
				}

				go func() {
					c.Consume(context.Background(), func(msg amqp.Delivery) error {
						receivedMessages = append(receivedMessages, msg)
						return msg.Ack(false)
					})
				}()

				// wait for to receive all the messages
				time.Sleep(1 * time.Second)

				dataReceived := [][]byte{}
				for _, msg := range receivedMessages {
					Expect(msg.Exchange).To(Equal(p.Config.Publisher.Exchange.ExchangeName))
					Expect(msg.RoutingKey).To(Equal(tName))
					Expect(msg.ConsumerTag).To(Equal(c.Config.Consumer.ConsumerTag))

					dataReceived = append(dataReceived, msg.Body)
				}

				Expect(dataReceived).To(Equal(data))
			})
		})

		// Context("consuming message with binding and unreliable conn", func() {
		// 	It("should reconnect & receive messages", func() {
		// 		qName := "something.binding.unreliable.service"
		// 		eName := "something.binding.unreliable.exchange"
		// 		tName := "something.created.unreliable"
		// 		receivedMessages := make([]amqp.Delivery, 0)

		// 		cnf.Consumer = &steadyrabbit.ConsumerConfig{
		// 			QueueConfig: &steadyrabbit.QueueConfig{
		// 				QueueName:    qName,
		// 				QueueDeclare: true,
		// 				QueueDurable: true,
		// 			},
		// 			Bindings: []*steadyrabbit.BindingConfig{
		// 				&steadyrabbit.BindingConfig{
		// 					Exchange: &steadyrabbit.ExchangeConfig{
		// 						ExchangeName:    eName,
		// 						ExchangeDeclare: true,
		// 						ExchangeType:    amqp.ExchangeTopic,
		// 					},
		// 					RoutingKeys: []string{tName},
		// 				},
		// 			},
		// 		}

		// 		c, err := steadyrabbit.NewConsumer(cnf)
		// 		Expect(err).ToNot(HaveOccurred())
		// 		Expect(c).ToNot(BeNil())

		// 		cnf.Publisher = &steadyrabbit.PublisherConfig{
		// 			Exchange: &steadyrabbit.ExchangeConfig{
		// 				ExchangeName:    eName,
		// 				ExchangeDeclare: true,
		// 				ExchangeType:    amqp.ExchangeTopic,
		// 			},
		// 		}
		// 		p, err := steadyrabbit.NewPublisher(cnf)
		// 		Expect(err).ToNot(HaveOccurred())

		// 		data := [][]byte{
		// 			[]byte("shahin"), []byte("shahin"), []byte("shahin"), []byte("shahin"),
		// 			[]byte("shahin"), []byte("shahin"), []byte("shahin"), []byte("shahin"),
		// 			[]byte("shahin"), []byte("shahin"), []byte("shahin"), []byte("shahin"),
		// 		}
		// 		for _, d := range data {
		// 			err = p.Publish(context.Background(), tName, d)
		// 			Expect(err).ToNot(HaveOccurred())
		// 		}

		// 		go func() {
		// 			c.Consume(context.Background(), func(msg amqp.Delivery) error {
		// 				receivedMessages = append(receivedMessages, msg)
		// 				return msg.Ack(false)
		// 			})
		// 		}()

		// 		// // Write an error to the NotifyCloseChan
		// 		c.NotifyCloseChan <- &amqp.Error{
		// 			Code:    0,
		// 			Reason:  "Test failure",
		// 			Server:  false,
		// 			Recover: false,
		// 		}

		// 		// wait for to receive all the messages
		// 		time.Sleep(2 * time.Second)

		// 		dataReceived := [][]byte{}
		// 		for _, msg := range receivedMessages {
		// 			Expect(msg.Exchange).To(Equal(p.Config.Publisher.Exchange.ExchangeName))
		// 			Expect(msg.RoutingKey).To(Equal(tName))
		// 			Expect(msg.ConsumerTag).To(Equal(c.Config.Consumer.ConsumerTag))

		// 			dataReceived = append(dataReceived, msg.Body)
		// 		}

		// 		Expect(dataReceived).To(Equal(data))
		// 	})
		// })

		Context("Performance Benchmark: ", func() {
			cnf := formPublisherConfig()
			qName := "something.binding.performance.service"
			eName := "something.binding.performance.exchange"
			tName := "something.created.performance"
			receivedMessages := make([]amqp.Delivery, 0)

			cnf.Consumer = &steadyrabbit.ConsumerConfig{
				QueueConfig: &steadyrabbit.QueueConfig{
					QueueName:    qName,
					QueueDeclare: true,
					QueueDurable: true,
				},
				Bindings: []*steadyrabbit.BindingConfig{
					&steadyrabbit.BindingConfig{
						Exchange: &steadyrabbit.ExchangeConfig{
							ExchangeName:    eName,
							ExchangeDeclare: true,
							ExchangeType:    amqp.ExchangeTopic,
						},
						RoutingKeys: []string{tName},
					},
				},
			}

			c, err := steadyrabbit.NewConsumer(cnf)
			Expect(err).ToNot(HaveOccurred())
			Expect(c).ToNot(BeNil())

			cnf.Publisher = &steadyrabbit.PublisherConfig{
				Exchange: &steadyrabbit.ExchangeConfig{
					ExchangeName:    eName,
					ExchangeDeclare: true,
					ExchangeType:    amqp.ExchangeTopic,
				},
			}
			p, err := steadyrabbit.NewPublisher(cnf)
			Expect(err).ToNot(HaveOccurred())

			data := []byte("shahin")

			go func() {
				c.Consume(context.Background(), func(msg amqp.Delivery) error {
					receivedMessages = append(receivedMessages, msg)
					return msg.Ack(false)
				})
			}()

			for i := 0; i < 25000; i++ {
				err = p.Publish(context.Background(), tName, data)
				Expect(err).ToNot(HaveOccurred())
			}

			Measure("it should be able to consume 5000 message per sec", func(b Benchmarker) {
				runtime := b.Time("runtime", func() {
					for {
						if len(receivedMessages) >= 15000 {
							receivedMessages = make([]amqp.Delivery, 0)
							break
						}
					}
				})

				Expect(runtime.Seconds()).Should(BeNumerically("<", 5), "5000 message consume shouldn't take too long.")
				//b.RecordValue("disk usage (in MB)", HowMuchDiskSpaceDidYouUse())
			}, 1)

		})

	})
})
