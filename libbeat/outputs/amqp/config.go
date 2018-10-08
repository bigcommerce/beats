package amqp

import (
	"errors"
	"runtime"

	"github.com/elastic/beats/libbeat/common/transport/tlscommon"
	"github.com/elastic/beats/libbeat/outputs/codec"
)

var (
	ErrNoHostsConfigured   = errors.New("no hosts configured")
	ErrBulkMaxSizeTooSmall = errors.New("bulk_max_size must be greater than or equal to 0")
	ErrMaxRetriesTooSmall  = errors.New("max_retries must be greater than or equal to 0")
)

type amqpConfig struct {
	Hosts                    []string              `config:"hosts" validate:"required"`
	TLS                      *tlscommon.Config     `config:"ssl"`
	ExchangeDeclare          exchangeDeclareConfig `config:"exchange_declare"`
	PersistentDeliveryMode   bool                  `config:"persistent_delivery_mode"`
	ContentType              string                `config:"content_type"`
	MandatoryPublish         bool                  `config:"mandatory_publish"`
	ImmediatePublish         bool                  `config:"immediate_publish"`
	BulkMaxSize              int                   `config:"bulk_max_size"`
	MaxRetries               int                   `config:"max_retries"`
	EventPrepareConcurrency  uint64                `config:"event_prepare_concurrency"`
	PendingPublishBufferSize uint64                `config:"pending_publish_buffer_size"`
	Codec                    codec.Config          `config:"codec"`
}

type exchangeDeclareConfig struct {
	Enabled    bool   `config:"enabled"`
	Passive    bool   `config:"passive"`
	Kind       string `config:"kind"`
	Durable    bool   `config:"durable"`
	AutoDelete bool   `config:"auto_delete"`
}

func defaultConfig() amqpConfig {
	return amqpConfig{
		Hosts:                    nil,
		MaxRetries:               3,
		BulkMaxSize:              2048,
		PendingPublishBufferSize: 2048,
		EventPrepareConcurrency:  uint64(runtime.GOMAXPROCS(-1)),
	}
}

func (c *amqpConfig) Validate() error {
	if len(c.Hosts) == 0 {
		return ErrNoHostsConfigured
	}

	if c.BulkMaxSize < 0 {
		return ErrBulkMaxSizeTooSmall
	}

	if c.MaxRetries < 0 {
		return ErrMaxRetriesTooSmall
	}

	return nil
}
