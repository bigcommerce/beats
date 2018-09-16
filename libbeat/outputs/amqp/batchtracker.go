package amqp

import (
	"sync"
	"sync/atomic"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"
)

type batchTracker struct {
	batch       *publisher.Batch
	client      *client
	counter     uint64
	retries     []*publisher.Event
	retriesLock sync.Mutex
	logger      *logp.Logger
	total       uint64
}

func (b *batchTracker) fail(event *publisher.Event, err error) {
	b.logger.Errorf("publish: %v", err)

	b.retriesLock.Lock()
	defer b.retriesLock.Unlock()

	if b.retries == nil {
		b.retries = []*publisher.Event{event}
	} else {
		b.retries = append(b.retries, event)
	}

	b.dec()
}

func (b *batchTracker) dec() {
	// to decrement x, do AddUint64(&x, ^uint64(0)) - stdlib atomic docs
	if atomic.AddUint64(&b.counter, ^uint64(0)) != 0 {
		return
	}

	// TODO: need to handle Nacks and other issues
	b.logger.Debugf("batch complete")
	batch := *b.batch

	batch.CancelledEvents()
	batch.ACK()
}
