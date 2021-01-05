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

// BindingConfig -> exchange and routing key
type BindingConfig struct {
	RoutingKeys []string
	Exchange    *ExchangeConfig
}

// ConsumerConfig ...
type ConsumerConfig struct {
	AutoAck          bool
	ConsumerTag      string
	QosPrefetchCount int
	QosPrefetchSize  int
	QueueConfig      *QueueConfig
	Bindings         []*BindingConfig
}

// PublisherConfig ...
type PublisherConfig struct {
	Exchange            *ExchangeConfig
	PublishConfirmation bool
	DeliveryMode        uint8
	Mandatory           bool
	Immediate           bool
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
		return errors.New("Config can not be nil")
	}

	if cnf.URL == "" {
		return errors.New("URL can not be empty")
	}

	if cnf.AppID == "" {
		cnf.AppID = DefaultAppID
	}

	if cnf.RetryReconnectIntervalSec == 0 {
		cnf.RetryReconnectIntervalSec = DefaultRetryReconnectIntervalSec
	}

	if cnf.Publisher == nil {
		cnf.Publisher = &PublisherConfig{
			DeliveryMode:        DefaultDeliveryMode,
			PublishConfirmation: DefaultPublisherConfirm,
			Immediate:           DefaultIsImmediatePublish,
			Mandatory:           DefaultIsMandatoryPublish,
		}

	}

	if cnf.Publisher.Exchange == nil {
		cnf.Publisher.Exchange = &ExchangeConfig{}
	}

	if cnf.Publisher.Exchange.ExchangeDeclare {
		if cnf.Publisher.Exchange.ExchangeType == "" {
			return errors.New("ExchangeType can not be empty if ExchangeDeclare set to true")
		}
		if cnf.Publisher.Exchange.ExchangeName == "" {
			return errors.New("ExchangeName can not be empty if ExchangeDeclare set to true")
		}
	}

	return nil
}

// ValidateConsumerConfig validates consumer config
func ValidateConsumerConfig(cnf *Config) error {
	if cnf == nil {
		return errors.New("Config can not be nil")
	}

	if cnf.URL == "" {
		return errors.New("URL can not be empty")
	}

	if cnf.AppID == "" {
		cnf.AppID = DefaultAppID
	}

	if cnf.RetryReconnectIntervalSec == 0 {
		cnf.RetryReconnectIntervalSec = DefaultRetryReconnectIntervalSec
	}

	if cnf.Consumer == nil {
		cnf.Consumer = &ConsumerConfig{
			AutoAck:          false,
			ConsumerTag:      DefaultConsumerTag,
			QosPrefetchCount: 0,
			QosPrefetchSize:  0,
			Bindings:         []*BindingConfig{},
		}
	}
	if cnf.Consumer.ConsumerTag == "" {
		cnf.Consumer.ConsumerTag = DefaultConsumerTag
	}

	if cnf.Consumer.QueueConfig == nil {
		cnf.Consumer.QueueConfig = &QueueConfig{
			QueueName:    DefaultQueueName,
			QueueDeclare: true,
			QueueDurable: true,
		}
	}
	if cnf.Consumer.QueueConfig.QueueName == "" {
		cnf.Consumer.QueueConfig.QueueName = DefaultQueueName
	}

	// if len(cnf.Consumer.Bindings) == 0 {
	// 	return errors.New("Consumer bindings can not be empty")
	// }

	for _, b := range cnf.Consumer.Bindings {
		if b.Exchange == nil {
			return errors.New("Exchange can not be empty")
		}

		if b.Exchange.ExchangeDeclare == true {
			if b.Exchange.ExchangeType == "" {
				return errors.New("ExchangeType can not be empty if ExchangeDeclare set to true")
			}
			if b.Exchange.ExchangeName == "" {
				return errors.New("ExchangeName can not be empty if ExchangeDeclare set to true")
			}
		}

		if len(b.RoutingKeys) == 0 {
			return errors.New("Routing keys can not be empty")
		}
	}

	return nil
}
