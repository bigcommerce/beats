package amqp

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elastic/beats/libbeat/publisher"

	"github.com/elastic/beats/libbeat/logp"
)

var batchCounter uint64

func newBatchTracker(batch publisher.Batch, parentLogger *logp.Logger) *batchTracker {
	counter := atomic.AddUint64(&batchCounter, 1)
	id := time.Now().Format("20060102150405") + "-" + strconv.FormatUint(counter, 10)
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

// batchTracker represents and manages in-flight batches of AMQP publishes.
type batchTracker struct {
	// id is an app-specific batch identifier used for diagnostic purposes
	id string

	// batch is the original beat batch being tracked and managed
	batch publisher.Batch

	// total is the total amount of messages in the batch
	total uint64

	// logger is the interface which should be used for logging batch activity
	logger *logp.Logger

	// retries holds the events which should be retried after batch completion
	retries []publisher.Event

	// retriesM is a mutex lock for retries; in most cases this is accessed in
	// one routine but edge cases like channel errors can cause concurrent calls
	// to batchTracker's retry methods
	retriesM sync.Mutex

	// counter is the count of completed events, regardless of success status
	counter uint64
}

// confirmEvent counts an event as successfully completed, though in effect this
// only increases an internal counter since batchTracker does not retain data
// for successful events.
func (b *batchTracker) confirmEvent() {
	b.logger.Debugf("batch event confirm")
	b.countEvent()
}

// retryEvent counts an event and stores it for retrying. Events to retry will
// be sent back to the beats core to handle.
func (b *batchTracker) retryEvent(event publisher.Event) {
	b.logger.Debugf("batch event retry")
	b.retriesM.Lock()
	b.retries = append(b.retries, event)
	b.retriesM.Unlock()
	b.countEvent()
}

// countEvent increments the internal event counter but should not be called
// externally. Use confirmEvent or retryEvent instead.
//
// countEvent will finalize the batch once the internal counter reaches the
// total event count.
func (b *batchTracker) countEvent() {
	count := atomic.AddUint64(&b.counter, 1)
	if count != b.total {
		return
	}
	b.finalize()
}

// finalize sends the appropriate signal back to the beats core based on the
// status of the batch.
func (b *batchTracker) finalize() {
	b.retriesM.Lock()
	defer b.retriesM.Unlock()

	if len(b.retries) > 0 {
		b.logger.Debugf("batch complete, retrying %v events", len(b.retries))
		b.batch.RetryEvents(b.retries)
		return
	}

	b.logger.Debugf("batch completed successfully")
	b.batch.ACK()
}
