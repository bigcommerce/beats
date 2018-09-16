package amqp

import (
	"fmt"
	"sync"
	"time"

	"github.com/nu7hatch/gouuid"

	"github.com/streadway/amqp"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/codec"
	"github.com/elastic/beats/libbeat/outputs/outil"
	"github.com/elastic/beats/libbeat/publisher"
	"github.com/elastic/beats/libbeat/testing"
)

type client struct {
	beat     beat.Info
	logger   *logp.Logger
	observer outputs.Observer
	writer   codec.Codec

	contentType          string
	dialURL              string
	exchangeDeclare      exchangeDeclareConfig
	exchangeNameSelector outil.Selector
	deliveryMode         uint8
	immediatePublish     bool
	mandatoryPublish     bool
	routingKeySelector   outil.Selector
	redactedURL          string

	channel        *amqp.Channel
	closeWaitGroup sync.WaitGroup
	connection     *amqp.Connection

	declaredExchanges     map[string]empty
	declaredExchangesLock sync.RWMutex

	pendingPublishes     map[uint64]*pendingPublish
	pendingPublishesLock sync.Mutex

	publishCounter     uint64
	publishCounterLock sync.Mutex
}

func newClient(
	observer outputs.Observer,
	beat beat.Info,
	writer codec.Codec,
	dialURL string,
	exchangeName outil.Selector,
	exchangeDeclare exchangeDeclareConfig,
	routingKey outil.Selector,
	persistentDeliveryMode bool,
	contentType string,
	mandatoryPublish bool,
	immediatePublish bool,
) (*client, error) {
	logger := logp.NewLogger("amqp")
	logger.Debugf("newClient")

	c := &client{
		observer:             observer,
		beat:                 beat,
		dialURL:              dialURL,
		exchangeNameSelector: exchangeName,
		exchangeDeclare:      exchangeDeclare,
		routingKeySelector:   routingKey,
		deliveryMode:         getDeliveryMode(persistentDeliveryMode),
		contentType:          contentType,
		mandatoryPublish:     mandatoryPublish,
		immediatePublish:     immediatePublish,
		writer:               writer,

		declaredExchanges: map[string]empty{},
		pendingPublishes:  map[uint64]*pendingPublish{},
	}

	// redact password from dial URL for logging
	parsedURI, err := amqp.ParseURI(dialURL)
	if err != nil {
		return nil, fmt.Errorf("parse dial URL: %v", err)
	}

	parsedURI.Password = ""
	c.redactedURL = parsedURI.String()
	c.logger = logger.With("dial_url", c.redactedURL)

	return c, nil
}

// String implements Stringer for this output.
func (c *client) String() string {
	return "amqp(" + c.redactedURL + ")"
}

// Connect initialises and commences this output.
func (c *client) Connect() error {
	c.logger.Debugf("connect")
	connection, err := c.dial()
	if err != nil {
		return fmt.Errorf("dial: %v", err)
	}

	c.logger.Debugf("create channel")
	channel, err := connection.Channel()
	if err != nil {
		// TODO: channel (re-)creation might need to be a thing this output
		//       manages transparently if the channel closes during client
		//       lifetime... needs testing
		connection.Close()
		return fmt.Errorf("channel create: %v", err)
	}

	// TODO: if client instances can be reused, data like declared exchanges
	// and pending-publish trackers should be reset here, perhaps refactoring
	// newClient to make that part reusable

	confirmations := make(chan amqp.Confirmation)
	c.closeWaitGroup.Add(1)
	go c.consumeConfirmations(confirmations)

	c.logger.Debugf("enable confirm mode")
	err = channel.Confirm(false)
	if err != nil {
		close(confirmations)
		return fmt.Errorf("AMQP confirm mode: %v", err)
	}

	c.logger.Debugf("subscribe to publish confirmations")
	channel.NotifyPublish(confirmations)

	returns := make(chan amqp.Return)
	c.closeWaitGroup.Add(1)
	go c.consumeReturns(returns)

	c.logger.Debugf("subscribe to publish returns")
	channel.NotifyReturn(returns)

	c.connection = connection
	c.channel = channel

	return nil
}

// Test implements a connection test for this output.
func (c *client) Test(d testing.Driver) {
	d.Run(c.String(), func(d testing.Driver) {
		conn, err := c.dial()
		d.Fatal("dial", err)
		defer conn.Close()
		defer c.Close()

		d.Info("server version", fmt.Sprintf("%d.%d", c.connection.Major, c.connection.Minor))
	})
}

// Close terminates this output and blocks until child routines are finished.
// A single, combined error is returned if any Closable sub-resource returns an
// error.
func (c *client) Close() error {
	var channelErr, connectionErr error

	if c.channel != nil {
		c.logger.Debugf("closing channel")
		channelErr = c.channel.Close()
		c.channel = nil
		c.logger.Debugf("channel closed")
	}

	c.logger.Debugf("waiting for child routines to finish")
	c.closeWaitGroup.Wait()
	c.logger.Debugf("child routines finished")

	if c.connection != nil {
		c.logger.Debugf("closing connection")
		connectionErr = c.connection.Close()
		c.connection = nil
		c.logger.Debugf("connection closed")
	}

	if channelErr != nil || connectionErr != nil {
		return fmt.Errorf("AMQP close, channel error: %v, connection error: %v", channelErr, connectionErr)
	}

	return nil
}

func (c *client) Publish(batch publisher.Batch) error {
	events := batch.Events()
	batchSize := len(events)
	if batchSize < 1 {
		c.logger.Debugf("skipping empty batch")
		return nil
	}

	tracker := &batchTracker{
		batch:   &batch,
		client:  c,
		counter: uint64(batchSize),
		total:   uint64(batchSize),
	}

	c.observer.NewBatch(batchSize)
	c.logger.Debugf("publish batch, size: %v", batchSize)

	for i := range events {
		event := &events[i]

		preparedEvent, err := c.prepareEvent(event)
		if err != nil {
			c.logger.Errorf("dropping due to error: %v", err)
			c.observer.Dropped(1)
			tracker.fail(event, err)
			continue
		}

		c.logger.Debugf("publish event")
		c.publishCounterLock.Lock()
		err = c.channel.Publish(preparedEvent.exchangeName, preparedEvent.routingKey, c.mandatoryPublish, c.immediatePublish, preparedEvent.publishing)
		if err != nil {
			c.observer.Failed(1)
			tracker.fail(event, err)

			if event.Guaranteed() {
				c.logger.Errorf("publish error: %v", err)
			} else {
				c.logger.Warnf("publish error: %v", err)
			}
		} else {
			c.logger.Debugf("event published")
			c.publishCounter++
			c.pendingPublishesLock.Lock()
			c.pendingPublishes[c.publishCounter] = &pendingPublish{
				batchTracker: tracker,
				event:        event,
			}
			c.pendingPublishesLock.Unlock()
		}
		c.publishCounterLock.Unlock()
	}

	c.logger.Debugf("batch published")

	return nil
}

func (c *client) dial() (*amqp.Connection, error) {
	c.logger.Debugf("dial")
	return amqp.Dial(c.dialURL)
}

func (c *client) consumeReturns(returns <-chan amqp.Return) {
	defer c.closeWaitGroup.Done()
	defer c.logger.Debugf("return handling routine finished")
	c.logger.Debugf("starting return handling routine")

	for ret := range returns {
		c.logger.Debugf("return received: %#v", ret) // TODO: check what to log here
		// TODO: handle returns
	}
}

func (c *client) consumeConfirmations(confirmations <-chan amqp.Confirmation) {
	defer c.closeWaitGroup.Done()
	defer c.logger.Debugf("confirmation handling routine finished")
	c.logger.Debugf("starting confirmation handling routine")

	for confirmation := range confirmations {
		c.logger.Debugf("confirmation received, delivery tag: %v, ack: %v", confirmation.DeliveryTag, confirmation.Ack)

		c.pendingPublishesLock.Lock()
		pending, ok := c.pendingPublishes[confirmation.DeliveryTag]
		if ok {
			delete(c.pendingPublishes, confirmation.DeliveryTag)
			c.pendingPublishesLock.Unlock()

			if confirmation.Ack {
				pending.batchTracker.dec()
			} else {
				pending.batchTracker.fail(pending.event, ErrNack)
			}
		} else {
			c.pendingPublishesLock.Unlock()
			c.logger.Warnf("received unexpected confirmation delivery tag: %v", confirmation.DeliveryTag)
		}
	}
}

func (c *client) prepareEvent(event *publisher.Event) (*preparedEvent, error) {
	exchangeName, err := c.exchangeNameSelector.Select(&event.Content)
	if err != nil {
		return nil, fmt.Errorf("exchange select: %v", err)
	}
	c.logger.Debugf("calculated exchange name: %v", exchangeName)

	err = c.ensureExchangeDeclared(exchangeName)
	if err != nil {
		return nil, fmt.Errorf("exchange declare: %v", err)
	}

	routingKey, err := c.routingKeySelector.Select(&event.Content)
	if err != nil {
		return nil, fmt.Errorf("routing key select: %v", err)
	}
	c.logger.Debugf("calculated routing key: %v", routingKey)

	serializedEvent, err := c.writer.Encode(c.beat.Beat, &event.Content)
	if err != nil {
		return nil, fmt.Errorf("serialize: %v", err)
	}
	c.logger.Debugf("event serialized, len: %v", len(serializedEvent))

	buf := make([]byte, len(serializedEvent))
	copy(buf, serializedEvent)

	messageId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("uuid: %v", err)
	}

	msg := amqp.Publishing{
		DeliveryMode: c.deliveryMode,
		Timestamp:    time.Now(),
		ContentType:  c.contentType,
		Body:         buf,
		MessageId:    messageId.String(),
	}

	return &preparedEvent{
		exchangeName: exchangeName,
		routingKey:   routingKey,
		publishing:   msg,
	}, nil
}

func (c *client) ensureExchangeDeclared(exchangeName string) error {
	c.declaredExchangesLock.RLock()
	_, declared := c.declaredExchanges[exchangeName]
	c.declaredExchangesLock.RUnlock()
	if declared {
		// exchange already declared
		return nil
	}

	c.declaredExchangesLock.Lock()
	defer c.declaredExchangesLock.Unlock()
	_, declared = c.declaredExchanges[exchangeName]
	if declared {
		// another writer won
		return nil
	}

	// we have a write lock: declare exchange, or at least mark it as declared

	if c.exchangeDeclare.Enabled {
		c.logger.Debugf("declare exchange, name: %v, kind: %v, durable: %v, auto-delete: %v", exchangeName, c.exchangeDeclare.Kind, c.exchangeDeclare.Durable, c.exchangeDeclare.AutoDelete)
		err := c.channel.ExchangeDeclare(exchangeName, c.exchangeDeclare.Kind, c.exchangeDeclare.Durable, c.exchangeDeclare.AutoDelete, false, false, nil)
		if err != nil {
			return err
		}
	} else {
		c.logger.Debugf("exchange declaring not enabled, will not declare: %v", exchangeName)
	}

	c.declaredExchanges[exchangeName] = empty{}

	return nil
}
