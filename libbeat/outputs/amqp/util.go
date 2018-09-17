package amqp

import (
	"errors"
	"time"

	"github.com/streadway/amqp"
)

var (
	ErrClosed     = errors.New("output closed")
	ErrNack       = errors.New("NACK received from AMQP")
	ErrNilChannel = errors.New("AMQP client channel is nil")
)

type timeFunc func() time.Time

type empty struct{}

type preparedEvent struct {
	incomingEvent      eventTracker
	exchangeName       string
	routingKey         string
	attempt            uint64
	outgoingPublishing amqp.Publishing
}

type pendingPublish struct {
	deliveryTag uint64
	event       preparedEvent
}

// getDeliveryMode returns an amqp Delivery Mode value based on a boolean-style
// "persistent?" input.
func getDeliveryMode(persistentDeliveryMode bool) uint8 {
	if persistentDeliveryMode {
		return amqp.Persistent
	}
	return amqp.Transient
}
