package amqp

import (
	"github.com/satori/go.uuid"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"
)

func newBatchTracker(batch publisher.Batch, parentLogger *logp.Logger) *batchTracker {
	id := uuid.NewV4().String()
	logger := parentLogger.With("batch_id", id)
	logger.Debugf("begin tracking batch")

	return &batchTracker{
		id:      id,
		batch:   batch,
		total:   uint64(len(batch.Events())),
		logger:  logger,
		retries: []publisher.Event{},
	}
}

type batchTracker struct {
	id      string
	batch   publisher.Batch
	total   uint64
	logger  *logp.Logger
	retries []publisher.Event
	counter uint64
}

func (b *batchTracker) confirmEvent() {
	b.logger.Debugf("batch event confirm")
	b.countEvent()
}

func (b *batchTracker) retryEvent(event publisher.Event) {
	b.logger.Debugf("batch event retry")
	b.retries = append(b.retries, event)
	b.countEvent()
}

func (b *batchTracker) countEvent() {
	b.counter++
	if b.counter != b.total {
		return
	}

	b.logger.Debugf("batch complete")
	b.finalize()
}

func (b *batchTracker) finalize() {
	if len(b.retries) < 1 {
		b.batch.ACK()
		return
	}

	b.batch.RetryEvents(b.retries)
}
