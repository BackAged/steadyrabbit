package steadyrabbit

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// Publisher defines rabbitmq publisher
type Publisher struct {
	Config            *Config
	Conn              *amqp.Connection
	NotifyCloseChan   chan *amqp.Error
	NotifyPublishChan chan amqp.Confirmation
	PublisherChannel  *amqp.Channel
	PublisherRWMutex  *sync.RWMutex
	closed            bool
	ctx               context.Context
	cancel            func()
	log               *logrus.Entry
}

// NewPublisher instantiate and return a publisher
func NewPublisher(cnf *Config) (*Publisher, error) {
	if err := ValidatePublisherConfig(cnf); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	conn, err := connect(cnf)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Publisher{
		Config:           cnf,
		Conn:             conn,
		NotifyCloseChan:  make(chan *amqp.Error),
		PublisherRWMutex: &sync.RWMutex{},
		ctx:              ctx,
		cancel:           cancel,
		log:              logrus.WithField("pkg", "rabbit"),
	}

	if p.Config.Publisher.PublishConfirmation {
		p.NotifyPublishChan = make(chan amqp.Confirmation, 100)
	}

	ch, err := p.newChannel()
	if err != nil {
		return nil, err
	}
	p.PublisherChannel = ch

	if p.Config.Publisher.PublishConfirmation {
		p.PublisherChannel.NotifyPublish(p.NotifyPublishChan)
	}
	conn.NotifyClose(p.NotifyCloseChan)

	// Launch connection watcher/reconnect
	go p.watchNotifyClose()

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

	select {
	case p.PublisherRWMutex.RLock():
	case <-ctx.Done():
		return errors.New("publish deadline exceeded")
	}

	defer p.PublisherRWMutex.RUnlock()

	ap := amqp.Publishing{
		DeliveryMode: DefaultDeliveryMode,
		Body:         body,
		AppId:        p.Config.AppID,
	}

	for _, opt := range opts {
		opt(&ap)
	}

	if err := p.PublisherChannel.Publish(
		p.Config.Publisher.Exchange.ExchangeName, routingKey,
		DefaultIsMandatoryPublish, DefaultIsImmediatePublish, ap); err != nil {
		return err
	}

	return nil
}

// Close closes thee connection
func (p *Publisher) Close() error {
	p.cancel()

	if err := p.Conn.Close(); err != nil {
		return fmt.Errorf("unable to close amqp connection: %s", err)
	}

	p.closed = true

	return nil
}

func (p *Publisher) watchNotifyPublish() {
	for cnfrm := range p.NotifyPublishChan {
		p.log.Debugf("server confirmation received  +%v\n", cnfrm)
	}
}

func (p *Publisher) watchNotifyClose() {
	for {
		errClose := <-p.NotifyCloseChan
		p.log.Debugf("received message on notify close channel: '%+v' (reconnecting)", errClose)

		// Acquire mutex to pause all publisher in the time of reconnecting AND prevent
		// access to the channel map
		p.PublisherRWMutex.Lock()

		var (
			attempts int
			conn     *amqp.Connection
			err      error
		)
		for {
			attempts++

			conn, err = connect(p.Config)
			if err != nil {
				p.log.Warningf("unable to complete reconnect: %s; retrying in %d sec", err, p.Config.RetryReconnectIntervalSec)
				time.Sleep(time.Duration(p.Config.RetryReconnectIntervalSec) * time.Second)
				continue
			}

			p.log.Debugf("successfully reconnected after %d attempts", attempts)
			break
		}

		// setting connection
		p.Conn = conn

		// Create and set a new notify close channel (since old one gets shutdown)
		p.NotifyCloseChan = make(chan *amqp.Error, 0)
		p.Conn.NotifyClose(p.NotifyCloseChan)

		// Update channel
		publisherChannel, err := p.newChannel()
		if err != nil {
			logrus.Errorf("unable to set new channel: %s", err)
			panic(fmt.Sprintf("unable to set new channel: %s", err))
		}
		p.PublisherChannel = publisherChannel

		if p.Config.Publisher.PublishConfirmation {
			p.NotifyPublishChan = make(chan amqp.Confirmation, 100)
			p.PublisherChannel.NotifyPublish(p.NotifyPublishChan)
		}

		// Unlock so that producers can begin publishing messages from this new channel
		p.PublisherRWMutex.Unlock()

		p.log.Debug("watchNotifyClose has completed successfully")
	}
}

// connect tries to establish a connection with rabbitmq server
func connect(cnf *Config) (*amqp.Connection, error) {
	var (
		conn *amqp.Connection
		err  error
	)

	if cnf.UseTLS {
		tlsConfig := &tls.Config{}

		if cnf.SkipVerifyTLS {
			tlsConfig.InsecureSkipVerify = true
		}

		conn, err = amqp.DialTLS(cnf.URL, tlsConfig)
	} else {
		conn, err = amqp.Dial(cnf.URL)
	}
	if err != nil {
		return nil, errors.Wrap(err, "unable to dial server")
	}

	return conn, nil
}

func (p *Publisher) newChannel() (*amqp.Channel, error) {
	if p.Conn == nil {
		return nil, errors.New("r.Conn is nil - did this get instantiated correctly? bug?")
	}

	ch, err := p.Conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate channel")
	}

	if p.Config.Publisher.Exchange.ExchangeDeclare {
		if err := ch.ExchangeDeclare(
			p.Config.Publisher.Exchange.ExchangeName,
			p.Config.Publisher.Exchange.ExchangeType,
			p.Config.Publisher.Exchange.ExchangeDurable,
			p.Config.Publisher.Exchange.ExchangeAutoDelete,
			false,
			false,
			nil,
		); err != nil {
			return nil, errors.Wrap(err, "unable to declare exchange")
		}
	}

	if p.Config.Publisher.PublishConfirmation {
		if err = ch.Confirm(false); err != nil {
			return nil, errors.Wrap(err, "unable to instantiate channel")
		}
	}

	return ch, nil
}
