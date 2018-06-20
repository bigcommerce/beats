package amqp

import (
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/codec"
	"github.com/elastic/beats/libbeat/outputs/outil"
)

type amqp struct {
	config amqpConfig
	topic  outil.Selector
}

var debugf = logp.MakeDebug("amqp")

func init() {
	outputs.RegisterType("amqp", makeAMQP)
}

func makeAMQP(
	beat beat.Info,
	observer outputs.Observer,
	cfg *common.Config,
) (outputs.Group, error) {
	config := amqpConfig{}
	if err := cfg.Unpack(&config); err != nil {
		return outputs.Fail(err)
	}

	// TODO: real values
	retry := 0

	codec, err := codec.CreateEncoder(beat, config.Codec)
	if err != nil {
		return outputs.Fail(err)
	}

	client, err := newAMQPClient(
		observer,
		beat,
		codec,
		config.DialURL,
		config.ExchangeName,
		config.ExchangeKind,
		config.ExchangeDurable,
		config.ExchangeAutoDelete,
		config.RoutingKey,
		config.ContentType,
		config.MandatoryPublish,
		config.ImmediatePublish,
	)

	if err != nil {
		return outputs.Fail(err)
	}

	return outputs.Success(config.BulkMaxSize, retry, client)
}
