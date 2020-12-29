package steadyrabbit

import (
	"errors"
)

// QueueConfig -> queue info
type QueueConfig struct {
	QueueName       string
	QueueDurable    bool
	QueueExclusive  bool
	QueueAutoDelete bool
	QueueDeclare    bool
}

// ExchangeConfig -> exchange info
type ExchangeConfig struct {
	ExchangeName       string
	ExchangeType       string
	ExchangeDurable    bool
	ExchangeAutoDelete bool
	ExchangeDeclare    bool
}

// Binding -> queue and routing key
type Binding struct {
	RoutingKeys []string
	Exchange    ExchangeConfig
}

// BindingConfig ..
type BindingConfig struct {
	QueueConfig QueueConfig
	Bindings    []Binding
}

// ConsumerConfig ...
type ConsumerConfig struct {
	AutoAck          bool
	ConsumerTag      string
	QosPrefetchCount int
	Bindings         []BindingConfig
}

// PublisherConfig ...
type PublisherConfig struct {
	Exchange            *ExchangeConfig
	PublishConfirmation bool
}

// Config steadyrabbit configuration
type Config struct {
	URL                       string
	RetryReconnectIntervalSec int
	AppID                     string
	UseTLS                    bool
	SkipVerifyTLS             bool
	Publisher                 *PublisherConfig
	Consumer                  *ConsumerConfig
}

// ValidatePublisherConfig validates config
func ValidatePublisherConfig(cnf *Config) error {
	if cnf == nil {
		return errors.New("Config cannot be nil")
	}

	if cnf.URL == "" {
		return errors.New("URL cannot be empty")
	}

	if cnf.AppID == "" {
		cnf.AppID = DefaultAppID
	}

	if cnf.RetryReconnectIntervalSec == 0 {
		cnf.RetryReconnectIntervalSec = DefaultRetryReconnectIntervalSec
	}

	if cnf.Publisher != nil && cnf.Publisher.Exchange != nil {
		if cnf.Publisher.Exchange.ExchangeDeclare {
			if cnf.Publisher.Exchange.ExchangeType == "" {
				return errors.New("ExchangeType cannot be empty if ExchangeDeclare set to true")
			}
		}

		if cnf.Publisher.Exchange.ExchangeName == "" {
			return errors.New("ExchangeName cannot be empty")
		}
	}

	return nil
}
