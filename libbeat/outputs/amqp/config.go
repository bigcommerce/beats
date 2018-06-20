package amqp

import (
	"github.com/elastic/beats/libbeat/outputs/codec"
)

type amqpConfig struct {
	DialURL            string       `config:"dial_url" validate:"required"`
	ExchangeName       string       `config:"exchange_name" validate:"required"`
	ExchangeKind       string       `config:"exchange_kind" validate:"required"`
	ExchangeDurable    bool         `config:"exchange_durable"`
	ExchangeAutoDelete bool         `config:"exchange_auto_delete"`
	RoutingKey         string       `config:"routing_key" validate:"required"`
	ContentType        string       `config:"content_type"`
	MandatoryPublish   bool         `config:"mandatory_publish"`
	ImmediatePublish   bool         `config:"immediate_publish"`
	BulkMaxSize        int          `config:"bulk_max_size"`
	Codec              codec.Config `config:"codec"`
}
