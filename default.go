package steadyrabbit

import (
	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

// all defaults
var (
	DefaultRetryReconnectIntervalSec = 60
	DefaultAppID                     = "steadyrabbit-" + uuid.NewV4().String()[0:8]
	DefaultConsumerTag               = "steadyrabbit-" + uuid.NewV4().String()[0:8]
	DefaultQueueName                 = "steadyrabbit-queue-" + uuid.NewV4().String()[0:8]
	DefaultPublisherConfirm          = false
	DefaultIsImmediatePublish        = false
	DefaultIsMandatoryPublish        = false
	DefaultDeliveryMode              = amqp.Persistent
)
