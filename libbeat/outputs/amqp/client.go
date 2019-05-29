package amqp

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/elastic/beats/libbeat/publisher"

	"github.com/gofrs/uuid"
	"github.com/streadway/amqp"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/codec"
	"github.com/elastic/beats/libbeat/outputs/outil"
	"github.com/elastic/beats/libbeat/testing"
)

type client struct {
	beat                     beat.Info
	logger                   *logp.Logger
	codecConfig              codec.Config
	observer                 outputs.Observer
	contentType              string
	dialURL                  string
	tlsConfig                *tls.Config
	dialTimeout              time.Duration
	exchangeDeclare          exchangeDeclareConfig
	exchangeNameSelector     outil.Selector
	deliveryMode             uint8
	immediatePublish         bool
	mandatoryPublish         bool
	routingKeySelector       outil.Selector
	redactedURL              string
	eventPrepareConcurrency  uint64
	pendingPublishBufferSize uint64
	channelMax               int
	frameSize                int
	heartbeat                time.Duration

	closed         bool
	closeLock      sync.RWMutex
	closeWaitGroup sync.WaitGroup

	incomingEvents chan eventTracker
	outgoingEvents chan preparedEvent
}

func newClient(
	observer outputs.Observer,
	beat beat.Info,
	codecConfig codec.Config,
	dialURL string,
	tlsConfig *tls.Config,
	dialTimeout time.Duration,
	exchangeName outil.Selector,
	exchangeDeclare exchangeDeclareConfig,
	routingKey outil.Selector,
	persistentDeliveryMode bool,
	contentType string,
	mandatoryPublish bool,
	immediatePublish bool,
	eventPrepareConcurrency uint64,
	pendingPublishBufferSize uint64,
	channelMax int,
	frameSize int,
	heartbeat time.Duration,
) (*client, error) {
	logger := logp.NewLogger("amqp")
	logger.Debugf("newClient")

	c := &client{
		observer:                 observer,
		beat:                     beat,
		codecConfig:              codecConfig,
		dialURL:                  dialURL,
		tlsConfig:                tlsConfig,
		dialTimeout:              dialTimeout,
		exchangeNameSelector:     exchangeName,
		exchangeDeclare:          exchangeDeclare,
		routingKeySelector:       routingKey,
		deliveryMode:             getDeliveryMode(persistentDeliveryMode),
		contentType:              contentType,
		mandatoryPublish:         mandatoryPublish,
		immediatePublish:         immediatePublish,
		eventPrepareConcurrency:  eventPrepareConcurrency,
		pendingPublishBufferSize: pendingPublishBufferSize,
		channelMax:               channelMax,
		frameSize:                frameSize,
		heartbeat:                heartbeat,
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

func (c *client) String() string {
	return "amqp(" + c.redactedURL + ")"
}

// Connect prepares this client for publishing events to an AMQP service, but
// does not eagerly establish a connection.
//
// Connectivity will be established as needed once Publish calls are made.
func (c *client) Connect() error {
	c.closeLock.RLock()
	if c.closed {
		c.closeLock.RUnlock()
		return ErrClosed
	}

	c.incomingEvents = make(chan eventTracker)
	c.outgoingEvents = make(chan preparedEvent)

	if err := c.startRoutines(); err != nil {
		c.closeLock.RUnlock()
		if closeErr := c.Close(); closeErr != nil {
			c.logger.Errorf("post-connect-error close error: %v", closeErr)
		}
		return fmt.Errorf("start: %v", err)
	}

	c.closeLock.RUnlock()
	return nil
}

// Test implements a connection test for this output.
func (c *client) Test(d testing.Driver) {
	d.Run(c.String(), func(d testing.Driver) {
		conn, err := c.dial()
		d.Fatal("dial", err)
		d.Info("server version", fmt.Sprintf("%d.%d", conn.Major, conn.Minor))
		if err = conn.Close(); err != nil {
			d.Warn("test connection close error", fmt.Sprintf("%v", err))
		}
		if err = c.Close(); err != nil {
			d.Warn("test client close error", fmt.Sprintf("%v", err))
		}
	})
}

// Close terminates this output and blocks until in-flight batches have
// completed.
//
// Close is safe to call multiple times.
//
// Close will cause subsequent Publish calls to fail. In-flight batches are not
// affected.
//
// Once closed, this output should cannot reused.
func (c *client) Close() error {
	c.logger.Debugf("closing output")

	c.closeLock.Lock()
	defer c.closeLock.Unlock()

	if c.closed {
		c.logger.Debugf("close called on closing / already-closed output")
		return nil
	}

	c.closed = true
	if c.incomingEvents != nil {
		c.logger.Debugf("closing incoming events channel")
		close(c.incomingEvents)
	} else {
		c.logger.Debugf("incoming events channel is nil, skipping close() call")
	}

	c.logger.Debugf("waiting for child routines to finish")
	c.closeWaitGroup.Wait()

	c.logger.Debugf("output closed")
	return nil
}

func (c *client) Publish(batch publisher.Batch) error {
	// Hold a read lock on the closed flag so that incomingEvents remains open
	// for this batch at least.
	c.closeLock.RLock()
	defer c.closeLock.RUnlock()
	if c.closed {
		return ErrClosed
	}

	// Start tracking this batch and put all of its events into the incoming
	// events queue.
	batchTracker := newBatchTracker(batch, c.logger)
	for _, event := range batch.Events() {
		c.incomingEvents <- newEventTracker(batchTracker, event)
	}

	return nil
}

// dial establishes and returns a connection to the AMQP service based on the
// client configuration.
func (c *client) dial() (*amqp.Connection, error) {
	// note: plain auth SASL can be set by amqp's Dial call, no need for it here
	c.logger.Debugf("dial")
	return amqp.DialConfig(c.dialURL, amqp.Config{
		ChannelMax:      c.channelMax,
		FrameSize:       c.frameSize,
		Heartbeat:       c.heartbeat,
		TLSClientConfig: c.tlsConfig,
		Dial: func(network string, addr string) (net.Conn, error) {
			// this conn function copied from amqp library (it's not exported to reuse/modify)
			conn, err := net.DialTimeout(network, addr, c.dialTimeout)
			if err != nil {
				return nil, err
			}

			// Heartbeating hasn't started yet, don't stall forever on a dead server.
			if err := conn.SetReadDeadline(time.Now().Add(c.dialTimeout)); err != nil {
				return nil, err
			}

			return conn, nil
		},
	})
}

// startRoutines starts any goroutine-based workers. The first error (if any)
// from child-routine-start functions is returned.
func (c *client) startRoutines() error {
	if err := c.startHandlingOutgoingEvents(); err != nil {
		return fmt.Errorf("start outgoing events: %v", err)
	}

	if err := c.startHandlingIncomingEvents(); err != nil {
		return fmt.Errorf("start incoming events: %v", err)
	}

	return nil
}

func (c *client) startHandlingIncomingEvents() error {
	if c.incomingEvents == nil {
		return errors.New("incoming events channel is nil")
	}

	c.closeWaitGroup.Add(1)

	// We support concurrency on event preparation since it may cause single-
	// CPU contention if the data and configuration is complex enough.

	var concurrency uint64 = 1
	if c.eventPrepareConcurrency > 1 {
		concurrency = c.eventPrepareConcurrency
	}

	var wg sync.WaitGroup
	for i := uint64(1); i <= concurrency; i++ {
		// Encoders are not safe for concurrent use: create one per routine.
		encoder, err := codec.CreateEncoder(c.beat, c.codecConfig)
		if err != nil {
			return fmt.Errorf("create encoder: %v", err)
		}

		wg.Add(1)
		go c.handleIncomingEvents(i, encoder, &wg)
	}

	go c.finalizeIncomingEventHandlers(&wg)

	return nil
}

func (c *client) finalizeIncomingEventHandlers(wg *sync.WaitGroup) {
	wg.Wait()
	c.logger.Debugf("incoming event handlers finished, closing outgoing events channel")
	close(c.outgoingEvents)
	c.closeWaitGroup.Done()
}

// handleIncomingEvents prepares incoming events for publishing and places them
// on an outgoing events queue.
func (c *client) handleIncomingEvents(workerId uint64, encoder codec.Codec, wg *sync.WaitGroup) {
	defer wg.Done()
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

	// concurrency not yet supported here, but when it is each concurrent
	// outgoing handler needs to manage its own channel, see ...
	// "Channels are not supposed to be shared for concurrent publishing (in this client and in general)."
	// this should not support concurrency: https://github.com/streadway/amqp/issues/208#issuecomment-244160130
	c.closeWaitGroup.Add(1)
	go c.handleOutgoingEvents()

	return nil
}

func (c *client) handleOutgoingEvents() {
	defer c.closeWaitGroup.Done()
	defer c.logger.Debugf("outgoing event worker finished")
	c.logger.Debugf("outgoing event worker started")

	var declarer exchangeDeclarer
	if c.exchangeDeclare.Enabled {
		declarer = c.declareExchange
	} else {
		c.logger.Infof("exchange declaration not enabled in config")
		declarer = func(_ amqpChannel, _ string) error { return nil }
	}

	// TODO: consider moving these loop innards into functions so we can use `defer` to better handle Close() calls
	for {
		connection, err := c.dial()
		if err != nil {
			// For now, consider dial failures as permanent, though we could
			// do some limited retries here if filebeat won't do that.
			c.logger.Errorf("dial: %v", err)
			return
		}

		for {
			c.logger.Debugf("channel create")
			channel, err := connection.Channel()
			if err != nil {
				// Channel create failed. Try again on a new connection.
				c.logger.Errorf("channel create: %v", err)
				break
			}

			go c.logErrors("channel error: ", channel.NotifyClose(make(chan *amqp.Error)))

			eventPublisher, err := newEventPublisher(c.logger, channel, declarer, c.outgoingEvents, c.pendingPublishBufferSize, c.mandatoryPublish, c.immediatePublish)
			if err != nil {
				// Publisher create failed. Perhaps setting the channel to
				// Confirm mode failed. Try again on a new channel, but ensure
				// the channel is closed, too.
				c.logger.Errorf("new publisher: %v", err)
				if closeErr := channel.Close(); closeErr != nil {
					c.logger.Errorf("post-new-publisher-error close error: %v", closeErr)
				}
				continue
			}

			err = eventPublisher.done()

			// No matter the reason, when the publisher finished, close the
			// channel. We can't reuse the channel on new publishers.
			if closeErr := channel.Close(); closeErr != nil {
				c.logger.Errorf("publisher: channel close error: %v", closeErr)
			}

			// When a publisher ends with no error, the assumption is that the
			// outgoingEvents chan is closed ...
			if err == nil {
				// ...  so we close the connection and finish up.
				if closeError := connection.Close(); closeError != nil {
					c.logger.Errorf("publisher: connection close error: %v", closeError)
				}
				return
			}

			// ... in other cases there will be some sort exchange-declare or
			// publish error, but there's still events to try and publish so we
			// loop to retry on a new channel (and maybe a new connection).
			c.logger.Errorf("publisher: %v", err)
		}

		if closeError := connection.Close(); closeError != nil {
			c.logger.Errorf("publisher: connection close error: %v", closeError)
		}
	}
}

// logErrors logs all amqp.Errors received over ch to the client's logger.
func (c *client) logErrors(prefix string, ch <-chan *amqp.Error) {
	for err := range ch {
		if err != nil {
			c.logger.Errorf(prefix+"%v", err.Error())
		}
	}
}

// prepareEvent converts an incoming beat event into a ready-to-publish (and
// track) AMQP publishing.
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

	messageID, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("uuid: %v", err)
	}

	return &preparedEvent{
		incomingEvent: incoming,
		exchangeName:  exchangeName,
		routingKey:    routingKey,
		outgoingPublishing: amqp.Publishing{
			Timestamp:    now(),
			DeliveryMode: c.deliveryMode,
			ContentType:  c.contentType,
			Body:         body,
			MessageId:    messageID.String(),
		},
	}, nil
}

// encodeEvent serializes a given event using the given encoder according to the
// client's current configuration.
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
// No duplication checks for repeated declarations are performed here. Callers
// of this function are expected to deal with that.
func (c *client) declareExchange(channel amqpChannel, exchangeName string) error {
	c.logger.Debugf("declare exchange, name: %v, kind: %v, durable: %v, auto-delete: %v, passive: %v", exchangeName, c.exchangeDeclare.Kind, c.exchangeDeclare.Durable, c.exchangeDeclare.AutoDelete, c.exchangeDeclare.Passive)
	if c.exchangeDeclare.Passive {
		return channel.ExchangeDeclarePassive(exchangeName, c.exchangeDeclare.Kind, c.exchangeDeclare.Durable, c.exchangeDeclare.AutoDelete, false, false, nil)
	}
	return channel.ExchangeDeclare(exchangeName, c.exchangeDeclare.Kind, c.exchangeDeclare.Durable, c.exchangeDeclare.AutoDelete, false, false, nil)
}
