package steadyrabbit_test

import (
	"context"
	"time"

	"github.com/BackAged/steadyrabbit"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/streadway/amqp"
)

var _ = Describe("Publisher", func() {
	var (
		cnf *steadyrabbit.Config
		// p   *steadyrabbit.Publisher
		// err error
	)

	JustBeforeEach(func() {
		cnf = formPublisherConfig()
		// p, err = steadyrabbit.NewPublisher(cnf)

		// Expect(err).ToNot(HaveOccurred())
		// Expect(p).ToNot(BeNil())
	})

	Describe("NewPublisher", func() {
		Context("instantiating", func() {
			It("should success with valid config", func() {
				p, err := steadyrabbit.NewPublisher(cnf)

				Expect(err).ToNot(HaveOccurred())
				Expect(p).ToNot(BeNil())
			})

			It("should success with exchange config", func() {
				cnf := formPublisherConfig()
				cnf.Publisher = &steadyrabbit.PublisherConfig{
					PublishConfirmation: true,
					Exchange: &steadyrabbit.ExchangeConfig{
						ExchangeDeclare:    true,
						ExchangeType:       amqp.ExchangeDirect,
						ExchangeName:       "shahin",
						ExchangeDurable:    false,
						ExchangeAutoDelete: false,
					},
				}
				p, err := steadyrabbit.NewPublisher(cnf)

				Expect(err).ToNot(HaveOccurred())
				Expect(p).ToNot(BeNil())
			})

			It("should error with missing config", func() {
				p, err := steadyrabbit.NewPublisher(nil)

				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(ContainSubstring("Config cannot be nil"))
				Expect(p).To(BeNil())
			})

			It("should error with unreachable rabbit server", func() {
				cnf := formPublisherConfig()
				cnf.URL = "amqp://bad-url"

				r, err := steadyrabbit.NewPublisher(cnf)

				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(ContainSubstring("unable to dial server"))
				Expect(r).To(BeNil())
			})

			It("should instantiates various internals", func() {
				cnf := formPublisherConfig()

				p, err := steadyrabbit.NewPublisher(cnf)

				Expect(err).To(BeNil())
				Expect(p).ToNot(BeNil())

				Expect(p.Conn).ToNot(BeNil())
				Expect(p.NotifyCloseChan).ToNot(BeNil())
				Expect(p.PublisherRWMutex).ToNot(BeNil())
				Expect(p.Config).ToNot(BeNil())

				if p.Config.Publisher.PublishConfirmation {
					Expect(p.PublisherChannel).ToNot(BeNil())
				}
			})

			It("should start NotifyCloseChan watcher", func() {
				cnf := formPublisherConfig()

				p, err := steadyrabbit.NewPublisher(cnf)

				Expect(err).To(BeNil())
				Expect(p).ToNot(BeNil())

				// Before we write errors to the notify channel, copy previous
				// conn and channels so we can compare them after reconnect
				oldConn := p.Conn
				oldNotifyCloseChan := p.NotifyCloseChan
				oldPublisherChan := p.PublisherChannel

				// Write an error to the NotifyCloseChan
				p.NotifyCloseChan <- &amqp.Error{
					Code:    0,
					Reason:  "Test failure",
					Server:  false,
					Recover: false,
				}

				// Give our watcher a moment to see the msg and cause a reconnect
				time.Sleep(100 * time.Millisecond)

				// We should've reconnected and got a new conn
				Expect(p.Conn).ToNot(BeNil())
				Expect(oldConn).ToNot(Equal(p.Conn))

				// We should also get new channels
				Expect(p.NotifyCloseChan).ToNot(BeNil())
				Expect(oldNotifyCloseChan).ToNot(Equal(p.NotifyCloseChan))

				Expect(p.PublisherChannel).ToNot(BeNil())
				Expect(oldPublisherChan).ToNot(Equal(p.PublisherChannel))
			})

			It("should start PublisherChannel watcher if publisher confirm is on", func() {
				cnf := formPublisherConfig()
				cnf.Publisher = &steadyrabbit.PublisherConfig{
					PublishConfirmation: true,
				}

				p, err := steadyrabbit.NewPublisher(cnf)

				Expect(err).To(BeNil())
				Expect(p).ToNot(BeNil())
				Expect(p.NotifyPublishChan).ToNot(BeNil())

				// Write an confirmation to the NotifyChan
				p.NotifyPublishChan <- amqp.Confirmation{
					Ack:         true,
					DeliveryTag: 1,
				}
			})

		})
	})

	Describe("Publish", func() {
		Context("with alive connection", func() {
			It("should success", func() {
				p, err := steadyrabbit.NewPublisher(cnf)

				Expect(err).ToNot(HaveOccurred())
				Expect(p).ToNot(BeNil())

				err = p.Publish(context.Background(), "shahin", []byte("shahin"))
				Expect(err).ToNot(HaveOccurred())
			})

			It("should success with exchange config", func() {
				cnf := formPublisherConfig()
				cnf.Publisher = &steadyrabbit.PublisherConfig{
					PublishConfirmation: true,
					Exchange: &steadyrabbit.ExchangeConfig{
						ExchangeDeclare:    true,
						ExchangeType:       amqp.ExchangeDirect,
						ExchangeName:       "shahin",
						ExchangeDurable:    false,
						ExchangeAutoDelete: false,
					},
				}
				p, err := steadyrabbit.NewPublisher(cnf)

				Expect(err).ToNot(HaveOccurred())
				Expect(p).ToNot(BeNil())
			})
		})
	})

	Describe("Close", func() {
		Context("when publisher is closed", func() {
			It("should error when try to publish", func() {
				p, err := steadyrabbit.NewPublisher(cnf)

				Expect(err).ToNot(HaveOccurred())
				Expect(p).ToNot(BeNil())

				p.Close()

				err = p.Publish(context.Background(), "shahin", []byte("shahin"))
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(steadyrabbit.ErrConnectionClosed))
			})
		})
	})
})
