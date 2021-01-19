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
		p   *steadyrabbit.Publisher
		err error
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
				Expect(err.Error()).To(ContainSubstring("Config can not be nil"))
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

			It("should start PublisherChannel watcher if publisher confirm is on", func() {
				cnf := formPublisherConfig()
				cnf.Publisher = &steadyrabbit.PublisherConfig{
					PublishConfirmation: true,
				}

				p, err := steadyrabbit.NewPublisher(cnf)

				Expect(err).To(BeNil())
				Expect(p).ToNot(BeNil())
			})

		})
	})

	Describe("Publish", func() {
		Context("with good connection", func() {
			It("should success", func() {
				cnf := formPublisherConfig()
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
		Context("with unreliable connection", func() {
			It("should reconnect & publish", func() {
				cnf := formPublisherConfig()
				p, err := steadyrabbit.NewPublisher(cnf)

				Expect(err).ToNot(HaveOccurred())
				Expect(p).ToNot(BeNil())

				// Write an error to the NotifyCloseChan
				p.GetNotifyCloseChannel() <- &amqp.Error{
					Code:    0,
					Reason:  "Test failure",
					Server:  false,
					Recover: false,
				}

				err = p.Publish(context.Background(), "shahin", []byte("shahin"))
				Expect(err).ToNot(HaveOccurred())
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

	Describe("Performance", func() {
		Context("Performance Benchmark: ", func() {
			cnf = formPublisherConfig()
			p, err = steadyrabbit.NewPublisher(cnf)

			Expect(err).ToNot(HaveOccurred())
			Expect(p).ToNot(BeNil())

			Measure("it should be able to publish 5000 message per sec", func(b Benchmarker) {
				runtime := b.Time("runtime", func() {
					for i := 0; i < 5000; i++ {
						err = p.Publish(context.Background(), "shahin", []byte("shahin"))
						Expect(err).ToNot(HaveOccurred())
					}
				})

				Expect(runtime.Seconds()).Should(BeNumerically("<", 1), "5000 message publish shouldn't take too long.")
				//b.RecordValue("disk usage (in MB)", HowMuchDiskSpaceDidYouUse())
				time.Sleep(1 * time.Second)
			}, 10)
		})
	})

})
