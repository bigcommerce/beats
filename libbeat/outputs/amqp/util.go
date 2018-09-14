package amqp

import (
	"sync/atomic"

	"github.com/elastic/beats/libbeat/publisher"
	"github.com/streadway/amqp"
)

type empty struct{}

type preparedEvent struct {
	exchangeName string
	routingKey   string
	publishing   amqp.Publishing
}

type batchTracker struct {
	batch   *publisher.Batch
	counter uint64
}

func (b *batchTracker) dec() uint64 {
	// to decrement x, do AddUint64(&x, ^uint64(0)) - stdlib atomic docs
	return atomic.AddUint64(&b.counter, ^uint64(0))
}

// getDeliveryMode returns an amqp Delivery Mode value based on a boolean-style
// "persistent?" input.
func getDeliveryMode(persistentDeliveryMode bool) uint8 {
	if persistentDeliveryMode {
		return amqp.Persistent
	}
	return amqp.Transient
}
