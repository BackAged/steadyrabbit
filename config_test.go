package steadyrabbit_test

import (
	"github.com/BackAged/steadyrabbit"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/streadway/amqp"
)

func formPublisherConfig() *steadyrabbit.Config {
	return &steadyrabbit.Config{
		URL: "amqp://localhost",
	}
}

var _ = Describe("Config", func() {
	var (
		cnf *steadyrabbit.Config
	)

	Describe("ValidatePublisherConfig", func() {
		Context("validation combination of config", func() {
			BeforeEach(func() {
				cnf = formPublisherConfig()
			})

			It("errors with nil options", func() {
				err := steadyrabbit.ValidatePublisherConfig(nil)
				Expect(err).To(HaveOccurred())
			})

			It("errors when URL is empty", func() {
				cnf.URL = ""

				err := steadyrabbit.ValidatePublisherConfig(cnf)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("URL can not be empty"))
			})

			It("only requires ExchangeType, ExchangeName Exchange Declare is true", func() {
				cnf.Publisher = &steadyrabbit.PublisherConfig{
					Exchange: &steadyrabbit.ExchangeConfig{
						ExchangeDeclare: true,
					},
				}
				err := steadyrabbit.ValidatePublisherConfig(cnf)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("ExchangeType can not be empty if ExchangeDeclare set to true"))

				cnf.Publisher = &steadyrabbit.PublisherConfig{
					Exchange: &steadyrabbit.ExchangeConfig{
						ExchangeDeclare: true,
						ExchangeType:    amqp.ExchangeDirect,
					},
				}
				err = steadyrabbit.ValidatePublisherConfig(cnf)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("ExchangeName can not be empty if ExchangeDeclare set to true"))

				cnf.Publisher = &steadyrabbit.PublisherConfig{
					Exchange: &steadyrabbit.ExchangeConfig{
						ExchangeDeclare: false,
					},
				}
				err = steadyrabbit.ValidatePublisherConfig(cnf)
				Expect(err).ToNot(HaveOccurred())
			})

			It("sets RetryConnectIntervalSec, AppID to default if unset", func() {
				cnf.RetryReconnectIntervalSec = 0
				cnf.AppID = ""

				err := steadyrabbit.ValidatePublisherConfig(cnf)
				Expect(err).ToNot(HaveOccurred())

				Expect(cnf.RetryReconnectIntervalSec).To(Equal(steadyrabbit.DefaultRetryReconnectIntervalSec))
				Expect(cnf.AppID).To(Equal(steadyrabbit.DefaultAppID))
			})
		})
	})
})
