package steadyrabbit

import (
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// SessionType defines for which the session would
// be used for
type SessionType string

// SessionTypes
const (
	PublisherSessionType SessionType = "Publisher"
	ConsumerSessionType  SessionType = "Consumer"
)

// Session hold a rabbitmq connection and channel
type Session struct {
	connection              *amqp.Connection
	channel                 *amqp.Channel
	consumerDeliveryChannel <-chan amqp.Delivery
	NotifyCloseChan         chan *amqp.Error
	NotifyPublishChan       chan amqp.Confirmation
	sessionRWMutex          *sync.RWMutex
	config                  *Config
	sessionType             SessionType
	log                     *logrus.Entry
}

// NewSession instantiates and return a session
func NewSession(cnf *Config, sessionType SessionType) (*Session, error) {
	s := &Session{
		sessionType:     sessionType,
		config:          cnf,
		NotifyCloseChan: make(chan *amqp.Error),
		sessionRWMutex:  &sync.RWMutex{},
		log:             logrus.WithField("pkg", "steadyrabbit"),
	}

	if err := s.connect(); err != nil {
		return nil, err
	}

	if err := s.newChannel(); err != nil {
		return nil, err
	}

	if s.sessionType == ConsumerSessionType {
		if err := s.newDeliveryChannel(); err != nil {
			return nil, err
		}
	}

	if s.sessionType == PublisherSessionType {
		if s.config.Publisher.PublishConfirmation {
			s.NotifyPublishChan = make(chan amqp.Confirmation, 100)
			s.channel.NotifyPublish(s.NotifyPublishChan)
		}
	}

	s.connection.NotifyClose(s.NotifyCloseChan)

	go s.watchNotifyClose()

	return s, nil
}

// GetChannel returns the channel
func (s *Session) GetChannel() *amqp.Channel {
	s.sessionRWMutex.RLock()
	defer s.sessionRWMutex.RUnlock()

	if s.channel == nil {
		s.log.Info("this shouldn't happen unless a bug")
		panic("unable to get channel")
	}

	return s.channel
}

// GetDeliveryChannel returns consumer delivery channel
func (s *Session) GetDeliveryChannel() <-chan amqp.Delivery {
	s.sessionRWMutex.RLock()
	defer s.sessionRWMutex.RUnlock()

	if s.channel == nil {
		s.log.Info("this shouldn't happen unless a bug")
		panic("unable to get channel")
	}

	return s.consumerDeliveryChannel
}

// GetNotifyCloseChannel  returns notify close channel
func (s *Session) GetNotifyCloseChannel() chan *amqp.Error {
	s.sessionRWMutex.RLock()
	defer s.sessionRWMutex.RUnlock()

	return s.NotifyCloseChan
}

// GetNotifyPublishChannel  returns notify publish channel
func (s *Session) GetNotifyPublishChannel() chan amqp.Confirmation {
	s.sessionRWMutex.RLock()
	defer s.sessionRWMutex.RUnlock()

	return s.NotifyPublishChan
}

// Close tears the connection down
// with channels and  delivery channels too
func (s *Session) Close() error {
	if s.connection == nil {
		return nil
	}

	if err := s.connection.Close(); err != nil {
		return fmt.Errorf("unable to close rabbitmq connection: %s", err)
	}

	return nil
}

// connect connects to rabbit server
func (s *Session) connect() error {
	var (
		conn *amqp.Connection
		err  error
	)

	s.log.Debug("connecting to rabbitmq server")
	if s.config.UseTLS {
		tlsConfig := &tls.Config{}

		if s.config.SkipVerifyTLS {
			tlsConfig.InsecureSkipVerify = true
		}

		conn, err = amqp.DialTLS(s.config.URL, tlsConfig)
	} else {
		conn, err = amqp.Dial(s.config.URL)
	}
	if err != nil {
		return errors.Wrap(err, "unable to dial server")
	}

	s.log.Debug("connected to rabbitmq server")
	s.connection = conn

	return nil
}

// newChannel establish a new channel on a rabbitmq connection
func (s *Session) newChannel() error {
	if s.connection == nil {
		return errors.New("can not create channel without active connection")
	}

	ch, err := s.connection.Channel()
	if err != nil {
		return errors.Wrap(err, "unable to instantiate channel")
	}

	s.channel = ch

	return nil
}

// newDeliveryChannel sets up a consumer delivery channel
func (s *Session) newDeliveryChannel() error {
	qCnf := s.config.Consumer.QueueConfig
	if qCnf.QueueDeclare {
		if _, err := s.channel.QueueDeclare(
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

	dlvrChnl, err := s.channel.Consume(
		s.config.Consumer.QueueConfig.QueueName,
		s.config.Consumer.ConsumerTag,
		s.config.Consumer.AutoAck,
		s.config.Consumer.QueueConfig.QueueExclusive,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	s.consumerDeliveryChannel = dlvrChnl

	return nil
}

func (s *Session) watchNotifyClose() {
	for {
		errClose := <-s.NotifyCloseChan
		s.log.Infof("received message on notify close channel: '%+v' (reconnecting)", errClose)

		// Acquire mutex for reconnecting and prevent
		// access to the channel
		s.sessionRWMutex.Lock()

		s.Close()

		var (
			attempts int
			err      error
		)
		for {
			attempts++

			if err = s.connect(); err != nil {
				s.log.Warningf("unable to complete reconnect: %s; retrying in %d sec", err, s.config.RetryReconnectIntervalSec)
				time.Sleep(time.Duration(s.config.RetryReconnectIntervalSec) * time.Second)
				continue
			}

			s.log.Infof("successfully reconnected after %d attempts", attempts)
			break
		}

		if err := s.newChannel(); err != nil {
			fmt.Printf("unable to set new channel: %s", err)
			panic(fmt.Sprintf("unable to set new channel: %s", err))
		}
		fmt.Println("here")
		if s.sessionType == ConsumerSessionType {
			if err := s.newDeliveryChannel(); err != nil {
				fmt.Printf("unable to set new delivery channel: %s", err)
				panic(fmt.Sprintf("unable to set new delivery channel: %s", err))
			}
		}
		fmt.Println("here2")

		// Create and set a new notify close channel (since old one gets shutdown)
		s.NotifyCloseChan = make(chan *amqp.Error)
		s.connection.NotifyClose(s.NotifyCloseChan)

		if s.sessionType == PublisherSessionType {
			if s.config.Publisher.PublishConfirmation {
				s.NotifyPublishChan = make(chan amqp.Confirmation, 100)
				s.channel.NotifyPublish(s.NotifyPublishChan)
			}
		}

		// Unlock so that channel is usable again
		s.sessionRWMutex.Unlock()

		s.log.Info("watchNotifyClose has completed successfully")
	}
}
