package steadyrabbit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// Consumer defines consumer
type Consumer struct {
	Config          *Config
	Conn            *amqp.Connection
	NotifyCloseChan chan *amqp.Error
	ConsumerChannel *amqp.Channel
	DeliveryStream  <-chan amqp.Delivery
	ConsumerRWMutex *sync.RWMutex
	closed          bool
	ctx             context.Context
	cancel          func()
	log             *logrus.Entry
}

// NewConsumer instantiate a consumer and returns it
func NewConsumer(cnf *Config) (*Consumer, error) {
	if err := ValidateConsumerConfig(cnf); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	conn, err := connect(cnf)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Consumer{
		Config:          cnf,
		Conn:            conn,
		NotifyCloseChan: make(chan *amqp.Error),
		ConsumerRWMutex: &sync.RWMutex{},
		ctx:             ctx,
		cancel:          cancel,
		log:             logrus.WithField("pkg", "rabbit"),
	}

	ch, err := c.newChannel()
	if err != nil {
		return nil, err
	}
	c.ConsumerChannel = ch

	deliveryStream, err := c.newDeliveryStream()
	if err != nil {
		logrus.Errorf("unable to set new delivery stream: %s", err)
		panic(fmt.Sprintf("unable to set new delivery stream: %s", err))
	}
	c.DeliveryStream = deliveryStream

	conn.NotifyClose(c.NotifyCloseChan)

	// Launch connection watcher/reconnect
	go c.watchNotifyClose()

	return c, nil
}

func (c *Consumer) watchNotifyClose() {
	for {
		errClose := <-c.NotifyCloseChan
		c.log.Debugf("received message on notify close channel: '%+v' (reconnecting)", errClose)
		//c.log.Println("started reconnecting")
		// Acquire mutex to pause all publisher in the time of reconnecting AND prevent
		// access to the channel map
		c.ConsumerRWMutex.Lock()

		c.Conn.Close()

		var (
			attempts int
			conn     *amqp.Connection
			err      error
		)
		for {
			attempts++

			conn, err = connect(c.Config)
			if err != nil {
				c.log.Warningf("unable to complete reconnect: %s; retrying in %d sec", err, c.Config.RetryReconnectIntervalSec)
				time.Sleep(time.Duration(c.Config.RetryReconnectIntervalSec) * time.Second)
				continue
			}

			c.log.Debugf("successfully reconnected after %d attempts", attempts)
			break
		}

		// setting connection
		c.Conn = conn
		// c.log.Println("re connected")

		// Update channel
		consumerChannel, err := c.newChannel()
		if err != nil {
			logrus.Errorf("unable to set new channel: %s", err)
			panic(fmt.Sprintf("unable to set new channel: %s", err))
		}
		c.ConsumerChannel = consumerChannel
		//c.log.Println("new consumer channel", c.ConsumerChannel)

		deliveryStream, err := c.newDeliveryStream()
		if err != nil {
			c.log.Println("new delivery stream", err)
			logrus.Errorf("unable to set new delivery stream: %s", err)
			panic(fmt.Sprintf("unable to set new delivery stream: %s", err))
		}
		c.DeliveryStream = deliveryStream
		//c.log.Println("new delivery stream", c.DeliveryStream)

		// Create and set a new notify close channel (since old one gets shutdown)
		c.NotifyCloseChan = make(chan *amqp.Error, 0)
		c.Conn.NotifyClose(c.NotifyCloseChan)
		//c.log.Println("new notify close", c.NotifyCloseChan)

		// Unlock so that consumers can begin consuming messages from this new channel
		c.ConsumerRWMutex.Unlock()
		c.log.Debug("watchNotifyClose has completed successfully")
	}
}

func (c *Consumer) newChannel() (*amqp.Channel, error) {
	if c.Conn == nil {
		return nil, errors.New("Conn is nil")
	}

	ch, err := c.Conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate channel")
	}

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
			return nil, errors.Wrap(err, "unable to declare queue")
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
			return nil, errors.Wrap(err, "unable to declare exchange")
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
				return nil, errors.Wrap(err, "unable to bind queue")
			}
		}
	}

	return ch, nil
}

func (c *Consumer) newDeliveryStream() (<-chan amqp.Delivery, error) {
	// Acquire lock (in case we are reconnecting and channels are being swapped)
	deliveryStream, err := c.ConsumerChannel.Consume(
		c.Config.Consumer.QueueConfig.QueueName,
		c.Config.Consumer.ConsumerTag,
		c.Config.Consumer.AutoAck,
		c.Config.Consumer.QueueConfig.QueueExclusive,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return deliveryStream, nil
}

func (c *Consumer) getDeliveryStream() <-chan amqp.Delivery {
	// Acquire lock (in case we are reconnecting and channels are being swapped)
	c.ConsumerRWMutex.RLock()
	defer c.ConsumerRWMutex.RUnlock()
	return c.DeliveryStream
}

// Close closes the connection
func (c *Consumer) Close() error {
	c.cancel()

	if err := c.Conn.Close(); err != nil {
		return fmt.Errorf("unable to close amqp connection: %s", err)
	}

	c.closed = true

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
		case msg := <-c.getDeliveryStream():
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
