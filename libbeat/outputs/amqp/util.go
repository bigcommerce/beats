package amqp

import (
	"errors"

	"github.com/elastic/beats/libbeat/publisher"
	"github.com/streadway/amqp"
)

var (
	ErrNack = errors.New("NACK received from AMQP")
)

type empty struct{}

type preparedEvent struct {
	exchangeName string
	routingKey   string
	publishing   amqp.Publishing
}

type pendingPublish struct {
	event        *publisher.Event
	batchTracker *batchTracker
}

// getDeliveryMode returns an amqp Delivery Mode value based on a boolean-style
// "persistent?" input.
func getDeliveryMode(persistentDeliveryMode bool) uint8 {
	if persistentDeliveryMode {
		return amqp.Persistent
	}
	return amqp.Transient
}
