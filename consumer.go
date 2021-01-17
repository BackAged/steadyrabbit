package steadyrabbit

import (
	"context"
	"fmt"
	"reflect"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// Consumer defines consumer
type Consumer struct {
	Config  *Config
	session *Session
	ctx     context.Context
	cancel  func()
	log     *logrus.Entry
}

// NewConsumer instantiate a consumer and returns it
func NewConsumer(cnf *Config) (*Consumer, error) {
	if err := ValidateConsumerConfig(cnf); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	ssn, err := NewSession(cnf, ConsumerSessionType)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Consumer{
		Config:  cnf,
		session: ssn,
		ctx:     ctx,
		cancel:  cancel,
		log:     logrus.WithField("pkg", "steadyrabbit"),
	}

	if c.configureChannelForConsume(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Consumer) configureChannelForConsume() error {
	ch := c.session.GetChannel()

	qCnf := c.Config.Consumer.QueueConfig
	if qCnf.QueueDeclare {
		if _, err := ch.QueueDeclare(
			qCnf.QueueName,
			qCnf.QueueDurable,
			qCnf.QueueAutoDelete,
			qCnf.QueueExclusive,
			false,
			nil,
		); err != nil {
			return errors.Wrap(err, "unable to declare queue")
		}
	}

	for _, b := range c.Config.Consumer.Bindings {
		if err := ch.ExchangeDeclare(
			b.Exchange.ExchangeName,
			b.Exchange.ExchangeType,
			b.Exchange.ExchangeDurable,
			b.Exchange.ExchangeAutoDelete,
			false,
			false,
			nil,
		); err != nil {
			return errors.Wrap(err, "unable to declare exchange")
		}
	}

	for _, b := range c.Config.Consumer.Bindings {
		for _, br := range b.RoutingKeys {
			if err := ch.QueueBind(
				qCnf.QueueName,
				br,
				b.Exchange.ExchangeName,
				false,
				nil,
			); err != nil {
				return errors.Wrap(err, "unable to bind queue")
			}
		}
	}

	return nil
}

// GetNotifyCloseChannel returns notify close channel
func (c *Consumer) GetNotifyCloseChannel() chan *amqp.Error {
	return c.session.GetNotifyCloseChannel()
}

// Close closes the connection
func (c *Consumer) Close() error {
	c.cancel()

	if err := c.session.Close(); err != nil {
		return err
	}

	return nil
}

// Stop stops consuming
func (c *Consumer) Stop() error {
	c.cancel()

	return nil
}

// ConsumerFunc defines consumer callback
type ConsumerFunc func(msg amqp.Delivery) error

// Consume message from consumer
func (c *Consumer) Consume(ctx context.Context, f ConsumerFunc) error {
	for {
		select {
		case msg := <-c.session.GetDeliveryChannel():
			if reflect.DeepEqual(msg, amqp.Delivery{}) {
				fmt.Println("closed delivery channel")
				continue
			}
			if err := f(msg); err != nil {
				c.log.Debugf("error during consume: %s", err)
			}
		case <-ctx.Done():
			c.log.Warning("stopped via context")
			break
		case <-c.ctx.Done():
			c.log.Warning("stopped via Stop()")
			break
		}
	}

	return nil
}

// ConsumeOne consume message one time
func (c *Consumer) ConsumeOne(ctx context.Context, f ConsumerFunc) error {
	select {
	case msg := <-c.session.GetDeliveryChannel():
		if err := f(msg); err != nil {
			c.log.Debugf("error during consume: %s", err)
		}
	case <-ctx.Done():
		c.log.Warning("stopped via context")
		break
	case <-c.ctx.Done():
		c.log.Warning("stopped via Stop()")
		break
	}

	return nil
}
