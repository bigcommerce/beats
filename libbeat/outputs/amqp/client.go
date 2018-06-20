package amqp

import (
	"time"

	libamqp "github.com/streadway/amqp"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/codec"
	"github.com/elastic/beats/libbeat/publisher"
)

func newAMQPClient(
	observer outputs.Observer,
	dialURL string,
	exchangeName string,
	exchangeKind string,
	exchangeDurable bool,
	exchangeAutoDelete bool,
	writer codec.Codec,
) (*client, error) {
	c := &client{
		observer:           observer,
		dialURL:            dialURL,
		exchangeName:       exchangeName,
		exchangeKind:       exchangeKind,
		exchangeDurable:    exchangeDurable,
		exchangeAutoDelete: exchangeAutoDelete,
		codec:              writer,
	}
	return c, nil
}

type client struct {
	observer           outputs.Observer
	dialURL            string
	exchangeName       string
	exchangeKind       string
	exchangeDurable    bool
	exchangeAutoDelete bool
	codec              codec.Codec

	connection *libamqp.Connection
	channel    *libamqp.Channel
}

func (c *client) Connect() error {
	debugf("connect: %v", c.dialURL)
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

func (c *client) Close() error {
	debugf("close: %v", c.dialURL)

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
	debugf("publish: %v", c.dialURL)

	events := batch.Events()
	c.observer.NewBatch(len(events))

	for range events {
		exchange := ""
		key := ""
		mandatory := true
		immediate := true
		contentType := "text/plain"

		msg := libamqp.Publishing{
			DeliveryMode: libamqp.Persistent,
			Timestamp:    time.Now(),
			ContentType:  contentType,
			Body:         []byte("foo"),
		}

		c.channel.Publish(exchange, key, mandatory, immediate, msg)
	}

	return nil
}
