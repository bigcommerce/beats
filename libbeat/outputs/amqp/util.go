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
	outgoingPublishing amqp.Publishing
}

type pendingPublish struct {
	deliveryTag uint64
	event       preparedEvent
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
