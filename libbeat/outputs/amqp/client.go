package amqp

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/satori/go.uuid"
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
	beat                    beat.Info
	logger                  *logp.Logger
	codecConfig             codec.Config
	observer                outputs.Observer
	contentType             string
	dialURL                 string
	exchangeDeclare         exchangeDeclareConfig
	exchangeNameSelector    outil.Selector
	deliveryMode            uint8
	immediatePublish        bool
	mandatoryPublish        bool
	routingKeySelector      outil.Selector
	redactedURL             string
	eventPrepareConcurrency uint64
	maxPublishAttempts      uint64

	closed         bool
	closeLock      sync.RWMutex
	closeWaitGroup sync.WaitGroup

	channel    *amqp.Channel
	connection *amqp.Connection

	//pendingPublishes     map[uint64]preparedEvent
	//pendingPublishesLock sync.RWMutex
	pendingPublishes chan pendingPublish

	publishCounter     uint64
	publishCounterLock sync.Mutex

	incomingEvents chan eventTracker
	outgoingEvents chan preparedEvent
}

func newClient(
	observer outputs.Observer,
	beat beat.Info,
	codecConfig codec.Config,
	dialURL string,
	exchangeName outil.Selector,
	exchangeDeclare exchangeDeclareConfig,
	routingKey outil.Selector,
	persistentDeliveryMode bool,
	contentType string,
	mandatoryPublish bool,
	immediatePublish bool,
	eventPrepareConcurrency uint64,
	pendingPublishBufferSize uint64,
	maxPublishAttempts uint64,
) (*client, error) {
	logger := logp.NewLogger("amqp")
	logger.Debugf("newClient")

	c := &client{
		observer:                observer,
		beat:                    beat,
		codecConfig:             codecConfig,
		dialURL:                 dialURL,
		exchangeNameSelector:    exchangeName,
		exchangeDeclare:         exchangeDeclare,
		routingKeySelector:      routingKey,
		deliveryMode:            getDeliveryMode(persistentDeliveryMode),
		contentType:             contentType,
		mandatoryPublish:        mandatoryPublish,
		immediatePublish:        immediatePublish,
		eventPrepareConcurrency: eventPrepareConcurrency,
		maxPublishAttempts:      maxPublishAttempts,

		//pendingPublishes: map[uint64]preparedEvent{},
		incomingEvents:   make(chan eventTracker),
		outgoingEvents:   make(chan preparedEvent),
		pendingPublishes: make(chan pendingPublish, pendingPublishBufferSize),
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
	if !c.exchangeDeclare.Enabled {
		c.logger.Infof("exchange declaration not enabled in config, events may fail to publish if destination exchanges do not exist")
	}

	connection, err := c.dial()
	if err != nil {
		return fmt.Errorf("dial: %v", err)
	}

	c.logger.Debugf("create channel")
	channel, err := connection.Channel()
	if err != nil {
		// TODO: channel (re-)creation might need to be a thing this output manages transparently if the channel closes during client lifetime... needs testing
		// TODO: consider creating a channel-owner/coordinator struct separate from the client since channels are not meant to be shared concurrently
		connection.Close()
		return fmt.Errorf("channel create: %v", err)
	}

	c.connection = connection
	c.channel = channel

	if err = c.startRoutines(); err != nil {
		c.Close()
		return err
	}

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
	c.logger.Debugf("closing output")

	c.closeLock.Lock()
	if c.closed {
		c.closeLock.Unlock()
		c.logger.Debugf("close called on closing / already-closed output")
		return nil
	}
	c.closed = true
	c.logger.Debugf("closing incomingEvents channel")
	close(c.incomingEvents)
	c.closeLock.Unlock()

	var channelErr, connectionErr error

	if c.channel != nil {
		c.logger.Debugf("closing amqp channel")
		channelErr = c.channel.Close()
		c.channel = nil
		c.logger.Debugf("amqp channel closed")
	}

	c.logger.Debugf("waiting for child routines to finish")
	c.closeWaitGroup.Wait()
	c.logger.Debugf("child routines finished")

	if c.connection != nil {
		c.logger.Debugf("closing amqp connection")
		connectionErr = c.connection.Close()
		c.connection = nil
		c.logger.Debugf("amqp connection closed")
	}

	c.logger.Debugf("output closed")

	if channelErr != nil || connectionErr != nil {
		return fmt.Errorf("AMQP close, channel error: %v, connection error: %v", channelErr, connectionErr)
	}

	return nil
}

func (c *client) Publish(batch publisher.Batch) error {
	// hold a read lock on the closed flag so that incomingEvents remains open
	// for this batch at least
	c.closeLock.RLock()
	defer c.closeLock.RUnlock()
	if c.closed {
		return ErrClosed
	}

	batchTracker := newBatchTracker(batch, c.logger)
	for _, event := range batch.Events() {
		c.incomingEvents <- newEventTracker(batchTracker, event)
	}

	return nil
}

//func (c *client) oldPublish(batch publisher.Batch) error {
//	events := batch.Events()
//	batchSize := len(events)
//	if batchSize < 1 {
//		c.logger.Debugf("skipping empty batch")
//		return nil
//	}
//
//	tracker := &batchTracker{
//		batch:   batch,
//		counter: uint64(batchSize),
//		total:   uint64(batchSize),
//	}
//
//	c.observer.NewBatch(batchSize)
//	c.logger.Debugf("publish batch, size: %v", batchSize)
//
//	for i := range events {
//		event := events[i]
//		eventTracker := newEventTracker(tracker, event)
//
//		outgoingEvent, err := c.prepareEvent(eventTracker, time.Now)
//		if err != nil {
//			c.logger.Errorf("dropping due to error: %v", err)
//			c.observer.Dropped(1)
//			tracker.failRetry(event, err)
//			continue
//		}
//
//		//err = c.ensureExchangeDeclared(outgoingEvent.exchangeName)
//		//if err != nil {
//		//	return fmt.Errorf("exchange declare: %v", err)
//		//}
//
//		c.logger.Debugf("publish event")
//		c.publishCounterLock.Lock()
//		err = c.channel.Publish(outgoingEvent.exchangeName, outgoingEvent.routingKey, c.mandatoryPublish, c.immediatePublish, outgoingEvent.outgoingPublishing)
//		if err != nil {
//			c.observer.Failed(1)
//			tracker.failRetry(event, err)
//
//			if event.Guaranteed() {
//				c.logger.Errorf("publish error: %v", err)
//			} else {
//				c.logger.Warnf("publish error: %v", err)
//			}
//		} else {
//			c.logger.Debugf("event published")
//			c.publishCounter++
//			c.pendingPublishesLock.Lock()
//			c.pendingPublishes[c.publishCounter] = &pendingPublish{
//				batchTracker: tracker,
//				event:        &event,
//			}
//			c.pendingPublishesLock.Unlock()
//		}
//		c.publishCounterLock.Unlock()
//	}
//
//	c.logger.Debugf("batch published")
//
//	return nil
//}

func (c *client) dial() (*amqp.Connection, error) {
	c.logger.Debugf("dial")
	return amqp.Dial(c.dialURL)
}

// startRoutines starts any goroutine-based workers. The first error (if any)
// from child-routine-start funcions is returned.
func (c *client) startRoutines() error {
	var err error

	if err = c.startHandlingConfirmations(); err != nil {
		return err
	}

	if err = c.startHandlingReturns(); err != nil {
		return err
	}

	if err = c.startHandlingOutgoingEvents(); err != nil {
		return err
	}

	if err = c.startHandlingIncomingEvents(); err != nil {
		return err
	}

	return nil
}

func (c *client) startHandlingReturns() error {
	if c.channel == nil {
		return ErrNilChannel
	}

	// the channel created here is not stored on our client struct since the
	// amqp library is responsible for closing it

	c.logger.Debugf("subscribe to publish returns")
	returns := c.channel.NotifyReturn(make(chan amqp.Return))

	c.closeWaitGroup.Add(1)
	go c.handleReturns(returns)

	return nil
}

func (c *client) handleReturns(returns <-chan amqp.Return) {
	defer c.closeWaitGroup.Done()
	defer c.logger.Debugf("returns worker finished")
	c.logger.Debugf("returns worker starting")

	for ret := range returns {
		c.logger.Debugf("return received: %#v", ret) // TODO: check what to log here
	}
}

// startHandlingConfirmations puts the current channel in Confirm mode and
// starts confirmation handlers in new goroutines. An error may be returned if
// the channel cannot be placed in Confirm mode.
func (c *client) startHandlingConfirmations() error {
	if c.channel == nil {
		return ErrNilChannel
	}

	c.logger.Debugf("enable confirm mode")
	if err := c.channel.Confirm(false); err != nil {
		return fmt.Errorf("AMQP confirm mode: %v", err)
	}

	// the channel created here is not stored on our client struct since the
	// amqp library is responsible for closing it

	c.logger.Debugf("subscribe to publish confirmations")
	confirmations := c.channel.NotifyPublish(make(chan amqp.Confirmation))

	c.closeWaitGroup.Add(1)
	go c.handleConfirmations(confirmations)

	return nil
}

// handleConfirmations processes data arriving on the given confirmations
// channel until the channel is closed.
func (c *client) handleConfirmations(confirmations <-chan amqp.Confirmation) {
	defer c.closeWaitGroup.Done()
	defer c.logger.Debugf("confirmations worker finished")
	c.logger.Debugf("confirmations worker starting")

	for confirmation := range confirmations {
		c.logger.Debugf("confirmation received, delivery tag: %v, ack: %v", confirmation.DeliveryTag, confirmation.Ack)

		// the amqp library guarantees that confirms are delivered in the same
		// order that Publish is called, so, the next item on the pending
		// channel should be what was just confirmed
		pending, open := <-c.pendingPublishes
		if !open {
			c.logger.Errorf("pendingPublishes channel is closed when we're still receiving confirmations!")
			c.Close()
			return
		}

		if pending.deliveryTag != confirmation.DeliveryTag {
			c.logger.Errorf("confirmation delivery tag %v does not match pending delivery tag %v!", confirmation.DeliveryTag, pending.deliveryTag)
			c.Close()
			return
		}

		if !confirmation.Ack {
			if c.maxPublishAttempts > 0 && pending.event.attempt >= c.maxPublishAttempts {
				pending.event.incomingEvent.batchTracker.retryEvent(pending.event.incomingEvent.event)
				continue
			}
			pending.event.attempt++
			c.outgoingEvents <- pending.event
			continue
		}

		c.logger.Debugf("confirmation matched pending publish, confirming event on batch: %v", pending.event.incomingEvent.batchTracker.id)
		pending.event.incomingEvent.batchTracker.confirmEvent()
	}
}

func (c *client) startHandlingIncomingEvents() error {
	if c.incomingEvents == nil {
		return errors.New("incomingEvents channel is nil")
	}

	var concurrency uint64 = 1
	if c.eventPrepareConcurrency > 1 {
		concurrency = c.eventPrepareConcurrency
	}

	// we support concurrency on event preparation since it may cause single-
	// cpu contention if the data and configuration is complex
	handlerWaitGroup := &sync.WaitGroup{}
	for i := uint64(1); i <= concurrency; i++ {
		encoder, err := codec.CreateEncoder(c.beat, c.codecConfig)
		if err != nil {
			return fmt.Errorf("create encoder: %v", err)
		}

		handlerWaitGroup.Add(1)
		go c.handleIncomingEvents(i, encoder, handlerWaitGroup)
	}

	c.closeWaitGroup.Add(1)
	go c.handleIncomingEventsShutdown(handlerWaitGroup)

	return nil
}

func (c *client) handleIncomingEventsShutdown(handlerWaitGroup *sync.WaitGroup) {
	defer c.closeWaitGroup.Done()
	handlerWaitGroup.Wait()
	c.logger.Debugf("closing outgoingEvents channel")
	close(c.outgoingEvents)
}

// handleIncomingEvents prepares incoming events for publishing and places them
// on an outgoing events queue.
func (c *client) handleIncomingEvents(workerId uint64, encoder codec.Codec, handlerWaitGroup *sync.WaitGroup) {
	defer handlerWaitGroup.Done()
	defer c.logger.Debugf("incoming event worker %v finished", workerId)
	c.logger.Debugf("incoming event worker %v starting", workerId)

	for incomingEvent := range c.incomingEvents {
		prepared, err := c.prepareEvent(encoder, incomingEvent, time.Now)
		if err != nil {
			c.logger.Errorf("event dropped: %v", err)
			continue
		}

		c.outgoingEvents <- *prepared
	}
}

func (c *client) startHandlingOutgoingEvents() error {
	if c.outgoingEvents == nil {
		return errors.New("outgoingEvents channel is nil")
	}

	// this should not support concurrency: https://github.com/streadway/amqp/issues/208#issuecomment-244160130
	// "Channels are not supposed to be shared for concurrent publishing (in this client and in general)."
	c.closeWaitGroup.Add(1)
	go c.handleOutgoingEvents()

	return nil
}

func (c *client) handleOutgoingEvents() {
	defer c.closeWaitGroup.Done()
	defer c.logger.Debugf("outgoing event worker finished")
	c.logger.Debugf("outgoing event worker started")

	declaredExchanges := map[string]empty{}
	var deliveryTag uint64

	var err error
	for outgoingEvent := range c.outgoingEvents {
		if c.exchangeDeclare.Enabled {
			if _, declared := declaredExchanges[outgoingEvent.exchangeName]; !declared {
				if err = c.declareExchange(c.channel, outgoingEvent.exchangeName); err != nil {
					// TODO: must drop/retry message if exchange-declare failed
				}
				declaredExchanges[outgoingEvent.exchangeName] = empty{}
			}
		}

		c.logger.Debugf("publish event")
		err = c.channel.Publish(outgoingEvent.exchangeName, outgoingEvent.routingKey, c.mandatoryPublish, c.immediatePublish, outgoingEvent.outgoingPublishing)
		if err != nil {
			// TODO: must drop/retry message if publish failed
			continue
		}

		deliveryTag++
		c.logger.Debugf("published event with delivery tag: %v", deliveryTag)
		c.pendingPublishes <- pendingPublish{
			deliveryTag: deliveryTag,
			event:       outgoingEvent,
		}
	}
}

// prepareEvent prepares an incoming event for publishing to AMQP based on the
// output's configuration.
func (c *client) prepareEvent(codec codec.Codec, incoming eventTracker, now timeFunc) (*preparedEvent, error) {
	content := &incoming.event.Content

	exchangeName, err := c.exchangeNameSelector.Select(content)
	if err != nil {
		return nil, fmt.Errorf("exchange select: %v", err)
	}
	c.logger.Debugf("calculated exchange name: %v", exchangeName)

	routingKey, err := c.routingKeySelector.Select(content)
	if err != nil {
		return nil, fmt.Errorf("routing key select: %v", err)
	}
	c.logger.Debugf("calculated routing key: %v", routingKey)

	body, err := c.encodeEvent(codec, content)
	if err != nil {
		return nil, fmt.Errorf("encode: %v", err)
	}

	return &preparedEvent{
		incomingEvent: incoming,
		exchangeName:  exchangeName,
		routingKey:    routingKey,
		attempt:       1,
		outgoingPublishing: amqp.Publishing{
			Timestamp:    now(),
			DeliveryMode: c.deliveryMode,
			ContentType:  c.contentType,
			Body:         body,
			MessageId:    uuid.NewV4().String(),
		},
	}, nil
}

func (c *client) encodeEvent(encoder codec.Codec, content *beat.Event) ([]byte, error) {
	serialized, err := encoder.Encode(c.beat.Beat, content)
	if err != nil {
		return nil, fmt.Errorf("serialize: %v", err)
	}

	buf := make([]byte, len(serialized))
	copy(buf, serialized)

	return buf, nil
}

// declareExchange declares an exchange according to the client's current
// configuration using the given channel.
//
// No duplication checks for repeated declarations are performed here. See
// handleOutgoingEvents.
func (c *client) declareExchange(channel *amqp.Channel, exchangeName string) error {
	c.logger.Debugf("declare exchange, name: %v, kind: %v, durable: %v, auto-delete: %v", exchangeName, c.exchangeDeclare.Kind, c.exchangeDeclare.Durable, c.exchangeDeclare.AutoDelete)
	return c.channel.ExchangeDeclare(exchangeName, c.exchangeDeclare.Kind, c.exchangeDeclare.Durable, c.exchangeDeclare.AutoDelete, false, false, nil)
}
