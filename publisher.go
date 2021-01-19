package steadyrabbit

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// Publisher defines rabbitmq publisher
type Publisher struct {
	Config  *Config
	session *Session
	closed  bool
	log     *logrus.Entry
}

// NewPublisher instantiate and return a publisher
func NewPublisher(cnf *Config) (*Publisher, error) {
	if err := ValidatePublisherConfig(cnf); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	ssn, err := NewSession(cnf, PublisherSessionType)
	if err != nil {
		return nil, err
	}

	p := &Publisher{
		Config:  cnf,
		session: ssn,
		log:     logrus.WithField("pkg", "steadyrabbit"),
	}

	if p.configureChannelForPublish(); err != nil {
		return nil, err
	}

	if p.Config.Publisher.PublishConfirmation {
		go p.watchNotifyPublish()
	}

	return p, nil
}

// PublishingOption gives publishing options
type PublishingOption func(*amqp.Publishing)

// WithReply option setter
func WithReply(replyExchange string) PublishingOption {
	return func(ap *amqp.Publishing) {
		ap.ReplyTo = replyExchange
	}
}

// WithCorrelationID sets correlation id to publising
func WithCorrelationID(corrID string) PublishingOption {
	return func(ap *amqp.Publishing) {
		ap.CorrelationId = corrID
	}
}

// WithDeliveryMode sets delivery mode
func WithDeliveryMode(deliveryMode uint8) PublishingOption {
	return func(ap *amqp.Publishing) {
		ap.DeliveryMode = deliveryMode
	}
}

// WithPriority sets priority
func WithPriority(priority uint8) PublishingOption {
	return func(ap *amqp.Publishing) {
		ap.Priority = priority
	}
}

// Publish publishes message
func (p *Publisher) Publish(ctx context.Context, routingKey string, body []byte, opts ...PublishingOption) error {
	if p.closed {
		return ErrConnectionClosed
	}

	ap := amqp.Publishing{
		DeliveryMode: DefaultDeliveryMode,
		Body:         body,
		AppId:        p.Config.AppID,
	}

	for _, opt := range opts {
		opt(&ap)
	}

	if err := p.session.GetChannel().Publish(
		p.Config.Publisher.Exchange.ExchangeName, routingKey,
		DefaultIsMandatoryPublish, DefaultIsImmediatePublish, ap); err != nil {
		return err
	}

	return nil
}

// Close closes thee connection
func (p *Publisher) Close() error {
	p.closed = true

	if err := p.session.Close(); err != nil {
		return err
	}

	return nil
}

// GetNotifyCloseChannel returns notify close channel
func (p *Publisher) GetNotifyCloseChannel() chan *amqp.Error {
	return p.session.GetNotifyCloseChannel()
}

func (p *Publisher) watchNotifyPublish() {
	for cnfrm := range p.session.GetNotifyPublishChannel() {
		p.log.Printf("server confirmation received  +%v\n", cnfrm.DeliveryTag)
	}
}

func (p *Publisher) configureChannelForPublish() error {
	if p.Config.Publisher.Exchange.ExchangeDeclare {
		if err := p.session.GetChannel().ExchangeDeclare(
			p.Config.Publisher.Exchange.ExchangeName,
			p.Config.Publisher.Exchange.ExchangeType,
			p.Config.Publisher.Exchange.ExchangeDurable,
			p.Config.Publisher.Exchange.ExchangeAutoDelete,
			false,
			false,
			nil,
		); err != nil {
			return errors.Wrap(err, "unable to declare exchange")
		}
	}

	if p.Config.Publisher.PublishConfirmation {
		if err := p.session.GetChannel().Confirm(false); err != nil {
			return errors.Wrap(err, "unable to instantiate publisher confirmation")
		}
	}
	return nil
}
