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

// session hold a rabbitmq connection and channel
type session struct {
	connection              *amqp.Connection
	channel                 *amqp.Channel
	consumerDeliveryChannel <-chan amqp.Delivery
	notifyCloseChan         chan *amqp.Error
	sessionRWMutex          *sync.RWMutex
	config                  *Config
	sessionType             SessionType
	log                     *logrus.Entry
}

func newSession(cnf *Config, sessionType SessionType) (*session, error) {
	s := &session{
		sessionType:     sessionType,
		config:          cnf,
		notifyCloseChan: make(chan *amqp.Error),
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

	s.connection.NotifyClose(s.notifyCloseChan)

	go s.watchNotifyClose()

	return s, nil
}

// GetChannel returns the channel
func (s *session) GetChannel() *amqp.Channel {
	s.sessionRWMutex.RLock()
	defer s.sessionRWMutex.RUnlock()

	if s.channel == nil {
		s.log.Debug("this shouldn't happen unless a bug")
		panic("unable to get channel")
	}

	return s.channel
}

// GetDeliveryChannel returns consumer delivery channel
func (s *session) GetDeliveryChannel() <-chan amqp.Delivery {
	s.sessionRWMutex.RLock()
	defer s.sessionRWMutex.RUnlock()

	if s.channel == nil {
		s.log.Debug("this shouldn't happen unless a bug")
		panic("unable to get channel")
	}

	return s.consumerDeliveryChannel
}

// Close tears the connection down
// with channels and  delivery channels too
func (s *session) Close() error {
	if s.connection == nil {
		return nil
	}

	if err := s.connection.Close(); err != nil {
		return fmt.Errorf("unable to close rabbitmq connection: %s", err)
	}

	return nil
}

// connect connects to rabbit server
func (s *session) connect() error {
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
func (s *session) newChannel() error {
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
func (s *session) newDeliveryChannel() error {
	qCnf := s.config.Consumer.QueueConfig
	if qCnf.QueueDeclare {
		if _, err := s.GetChannel().QueueDeclare(
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

	dlvrChnl, err := s.GetChannel().Consume(
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

func (s *session) watchNotifyClose() {
	for {
		errClose := <-s.notifyCloseChan
		s.log.Debugf("received message on notify close channel: '%+v' (reconnecting)", errClose)

		// Acquire mutex for reconnecting and prevent
		// access to the channel
		s.sessionRWMutex.Lock()

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

			s.log.Debugf("successfully reconnected after %d attempts", attempts)
			break
		}

		// Create and set a new notify close channel (since old one gets shutdown)
		s.notifyCloseChan = make(chan *amqp.Error, 0)
		s.connection.NotifyClose(s.notifyCloseChan)

		if err := s.newChannel(); err != nil {
			logrus.Errorf("unable to set new channel: %s", err)
			panic(fmt.Sprintf("unable to set new channel: %s", err))
		}

		if s.sessionType == ConsumerSessionType {
			if err := s.newDeliveryChannel(); err != nil {
				logrus.Errorf("unable to set new delivery channel: %s", err)
				panic(fmt.Sprintf("unable to set new delivery channel: %s", err))
			}
		}

		// Unlock so that channel is usable again
		s.sessionRWMutex.Unlock()

		s.log.Debug("watchNotifyClose has completed successfully")
	}
}
