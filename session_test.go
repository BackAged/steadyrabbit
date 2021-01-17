package steadyrabbit_test

import (
	"fmt"
	"time"

	"github.com/BackAged/steadyrabbit"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/streadway/amqp"
)

func formConfig() *steadyrabbit.Config {
	return &steadyrabbit.Config{
		URL: "amqp://localhost",
	}
}

var _ = Describe("Session", func() {
	Context("NewSession", func() {
		It("should instantiates various internals", func() {
			cnf := formConfig()
			cnf.Consumer = &steadyrabbit.ConsumerConfig{
				QueueConfig: &steadyrabbit.QueueConfig{
					QueueDeclare: true,
					QueueName:    "session.queue",
				},
			}

			s, err := steadyrabbit.NewSession(cnf, steadyrabbit.ConsumerSessionType)

			Expect(err).To(BeNil())
			Expect(s).ToNot(BeNil())

			Expect(s.NotifyCloseChan).ToNot(BeNil())
			Expect(s.GetChannel()).ToNot(BeNil())
			Expect(s.GetDeliveryChannel()).ToNot(BeNil())
		})
	})

	It("should start NotifyCloseChan watcher", func() {
		cnf := formConfig()
		cnf.Consumer = &steadyrabbit.ConsumerConfig{
			QueueConfig: &steadyrabbit.QueueConfig{
				QueueDeclare: true,
				QueueName:    "session.queue",
			},
		}

		s, err := steadyrabbit.NewSession(cnf, steadyrabbit.ConsumerSessionType)

		Expect(err).To(BeNil())
		Expect(s).ToNot(BeNil())

		oldChannel := s.GetChannel()
		oldDeliveryChannel := s.GetDeliveryChannel()
		oldNotifyCloseChan := s.NotifyCloseChan

		// Write an error to the NotifyCloseChan
		s.NotifyCloseChan <- &amqp.Error{
			Code:    0,
			Reason:  "Test failure",
			Server:  false,
			Recover: false,
		}

		time.Sleep(2 * time.Second)

		// We should also get new channels
		Expect(s.NotifyCloseChan).ToNot(BeNil())
		Expect(oldNotifyCloseChan).ToNot(Equal(s.NotifyCloseChan))

		fmt.Println("got notify close channel")

		Expect(s.GetChannel()).ToNot(BeNil())
		Expect(oldChannel).ToNot(Equal(s.GetChannel()))

		fmt.Println("got channel")

		Expect(s.GetDeliveryChannel()).ToNot(BeNil())
		Expect(oldDeliveryChannel).ToNot(Equal(s.GetDeliveryChannel()))

		fmt.Println("got delivery channel")
	})
})
