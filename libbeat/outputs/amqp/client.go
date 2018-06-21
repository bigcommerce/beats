package amqp

import (
	"fmt"
	"time"

	libamqp "github.com/streadway/amqp"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/codec"
	"github.com/elastic/beats/libbeat/publisher"
	"github.com/elastic/beats/libbeat/testing"
)

func newAMQPClient(
	observer outputs.Observer,
	beat beat.Info,
	writer codec.Codec,
	dialURL string,
	exchangeName string,
	exchangeKind string,
	exchangeDurable bool,
	exchangeAutoDelete bool,
	routingKey string,
	contentType string,
	mandatoryPublish bool,
	immediatePublish bool,
) (*client, error) {
	c := &client{
		observer:           observer,
		beat:               beat,
		dialURL:            dialURL,
		exchangeName:       exchangeName,
		exchangeKind:       exchangeKind,
		exchangeDurable:    exchangeDurable,
		exchangeAutoDelete: exchangeAutoDelete,
		routingKey:         routingKey,
		contentType:        contentType,
		mandatoryPublish:   mandatoryPublish,
		immediatePublish:   immediatePublish,
		codec:              writer,
	}
	return c, nil
}

type client struct {
	observer outputs.Observer
	beat     beat.Info
	codec    codec.Codec

	dialURL            string
	exchangeName       string
	exchangeKind       string
	exchangeDurable    bool
	exchangeAutoDelete bool
	routingKey         string
	contentType        string
	mandatoryPublish   bool
	immediatePublish   bool

	connection *libamqp.Connection
	channel    *libamqp.Channel
}

func (c *client) Connect() error {
	debugf("connect")
	connection, err := libamqp.Dial(c.dialURL)
	if err != nil {
		logp.Err("AMQP connection error: %v", err)
		return err
	}

	debugf("create channel")
	channel, err := connection.Channel()
	if err != nil {
		// TODO: channel (re-)creation might need to be a thing this output
		//       manages transparently
		logp.Err("AMQP channel creation error: %v", err)
		connection.Close()
		return err
	}

	debugf("declare exchange, name: %v, kind: %v, durable: %v, auto-delete: %v", c.exchangeName, c.exchangeKind, c.exchangeDurable, c.exchangeAutoDelete)
	err = channel.ExchangeDeclare(c.exchangeName, c.exchangeKind, c.exchangeDurable, c.exchangeAutoDelete, false, false, nil)
	if err != nil {
		logp.Err("AMQP exchange declaration error: %v", err)
		channel.Close()
		connection.Close()
		return err
	}

	c.connection = connection
	c.channel = channel

	return nil
}

func (c *client) Test(d testing.Driver) {
	d.Run("amqp: "+c.dialURL, func(d testing.Driver) {
		d.Fatal("connect", c.Connect())
		defer c.Close()

		d.Info("server version", fmt.Sprintf("%d.%d", c.connection.Major, c.connection.Minor))
	})
}

func (c *client) Close() error {
	debugf("close")

	if c.channel != nil {
		err := c.channel.Close()
		c.channel = nil

		if err != nil {
			logp.Err("AMQP channel close fails with: %v", err)
			return err
		}
	}

	if c.connection != nil {
		err := c.connection.Close()
		c.connection = nil

		if err != nil {
			logp.Err("AMQP connection close fails with: %v", err)
			return err
		}
	}

	return nil
}

func (c *client) Publish(batch publisher.Batch) error {
	debugf("publish batch")

	events := batch.Events()
	c.observer.NewBatch(len(events))

	for i := range events {
		event := &events[i]

		serializedEvent, err := c.codec.Encode(c.beat.Beat, &event.Content)
		if err != nil {
			if event.Guaranteed() {
				logp.Critical("Failed to serialize event: %v", err)
			} else {
				logp.Warn("Failed to serialize event: %v", err)
			}

			c.observer.Dropped(1)
			continue
		}

		buf := make([]byte, len(serializedEvent))
		copy(buf, serializedEvent)

		msg := libamqp.Publishing{
			DeliveryMode: libamqp.Persistent, // TODO: does this need to be configurable?
			Timestamp:    time.Now(),
			ContentType:  c.contentType,
			Body:         buf,
		}

		debugf("publish")

		err = c.channel.Publish(c.exchangeName, c.routingKey, c.mandatoryPublish, c.immediatePublish, msg)
		if err != nil {
			if event.Guaranteed() {
				logp.Critical("Failed to publish event: %v", err)
			} else {
				logp.Warn("Failed to publish event: %v", err)
			}

			c.observer.Dropped(1)
			continue
		}
	}

	return nil
}
