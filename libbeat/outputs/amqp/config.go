package amqp

import (
	"errors"

	"github.com/elastic/beats/libbeat/outputs/codec"
)

var (
	ErrNoHostsConfigured   = errors.New("no hosts configured")
	ErrBulkMaxSizeTooSmall = errors.New("bulk_max_size must be greater than or equal to 0")
)

type amqpConfig struct {
	Hosts []string `config:"hosts" validate:"required"`

	ExchangeDeclare          exchangeDeclareConfig `config:"exchange_declare"`
	RoutingKey               string                `config:"routing_key" validate:"required"`
	PersistentDeliveryMode   bool                  `config:"persistent_delivery_mode"`
	ContentType              string                `config:"content_type"`
	MandatoryPublish         bool                  `config:"mandatory_publish"`
	ImmediatePublish         bool                  `config:"immediate_publish"`
	BulkMaxSize              int                   `config:"bulk_max_size"`
	EventPrepareConcurrency  uint64                `config:"event_prepare_concurrency"`
	PendingPublishBufferSize uint64                `config:"pending_publish_buffer_size"`
	MaxPublishAttempts       uint64                `config:"max_publish_attempts"`

	Codec codec.Config `config:"codec"`
}

type exchangeDeclareConfig struct {
	Enabled    bool   `config:"enabled"`
	Kind       string `config:"kind"`
	Durable    bool   `config:"durable"`
	AutoDelete bool   `config:"auto_delete"`
}

func defaultConfig() amqpConfig {
	return amqpConfig{
		Hosts:       nil,
		BulkMaxSize: 2048,
	}
}

func (c *amqpConfig) Validate() error {
	if len(c.Hosts) == 0 {
		return ErrNoHostsConfigured
	}

	if c.BulkMaxSize < 0 {
		return ErrBulkMaxSizeTooSmall
	}

	return nil
}
