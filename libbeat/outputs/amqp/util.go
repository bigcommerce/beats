package amqp

import (
	"errors"
	"time"

	"github.com/streadway/amqp"
)

var (
	ErrClosed                        = errors.New("output closed")
	ErrNack                          = errors.New("NACK received from AMQP")
	ErrNilChannel                    = errors.New("AMQP client channel is nil")
	ErrConfirmationsClosed           = errors.New("confirmations channel closed with a pending publish")
	ErrConfirmationsClosedWithReturn = errors.New("confirmations channel closed with a pending publish and orphaned return signal")
)

type timeFunc func() time.Time

type empty struct{}

type preparedEvent struct {
	incomingEvent      eventTracker
	exchangeName       string
	routingKey         string
	outgoingPublishing amqp.Publishing
}

type pendingPublish struct {
	deliveryTag uint64
	event       preparedEvent
}

// retry is a convenience function for signalling a retry back to the origin
// batch for this pending publish.
func (p pendingPublish) retry() {
	p.event.incomingEvent.batchTracker.retryEvent(p.event.incomingEvent.event)
}

// retry is a convenience function for signalling a confirmation back to the
// origin batch for this pending publish.
func (p pendingPublish) confirm() {
	p.event.incomingEvent.batchTracker.confirmEvent()
}

type exchangeDeclarer func(amqpChannel, string) error

type amqpConnector interface {
	Connect() (*amqp.Connection, error)
}

type amqpConnection interface {
	Channel() (*amqp.Channel, error)
	Close() error
}

type amqpChannel interface {
	Close() error
	Confirm(noWait bool) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	NotifyReturn(c chan amqp.Return) chan amqp.Return
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
}

// getDeliveryMode returns an amqp Delivery Mode value based on a boolean-style
// "persistent?" input.
func getDeliveryMode(persistentDeliveryMode bool) uint8 {
	if persistentDeliveryMode {
		return amqp.Persistent
	}
	return amqp.Transient
}
