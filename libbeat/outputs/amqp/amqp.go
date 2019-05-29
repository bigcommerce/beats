package amqp

import (
	"crypto/tls"
	"fmt"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/outil"
)

func init() {
	outputs.RegisterType("amqp", makeAMQP)
}

func makeAMQP(
	_ outputs.IndexManager,
	beat beat.Info,
	observer outputs.Observer,
	cfg *common.Config,
) (outputs.Group, error) {
	logger := logp.NewLogger("amqp")
	logger.Debugf("initialize amqp output")

	config := defaultConfig()
	if err := cfg.Unpack(&config); err != nil {
		return outputs.Fail(fmt.Errorf("config: %v", err))
	}

	exchangeSelector, err := outil.BuildSelectorFromConfig(cfg, outil.Settings{
		Key:              "exchange",
		MultiKey:         "exchanges",
		EnableSingleOnly: true,
		FailEmpty:        true,
	})
	if err != nil {
		return outputs.Fail(fmt.Errorf("exchange: %v", err))
	}

	routingKeySelector, err := outil.BuildSelectorFromConfig(cfg, outil.Settings{
		Key:              "routing_key",
		MultiKey:         "routing_keys",
		EnableSingleOnly: true,
		FailEmpty:        true,
	})
	if err != nil {
		return outputs.Fail(fmt.Errorf("routing key: %v", err))
	}

	hosts, err := outputs.ReadHostList(cfg)
	if err != nil {
		return outputs.Fail(fmt.Errorf("host list: %v", err))
	}

	tlsConfig, err := outputs.LoadTLSConfig(config.TLS)
	if err != nil {
		return outputs.Fail(fmt.Errorf("tls: %v", err))
	}

	var amqpTlsConfig *tls.Config
	if tlsConfig != nil {
		amqpTlsConfig = tlsConfig.BuildModuleConfig("")
	}

	clients := make([]outputs.NetworkClient, len(hosts))
	for i, host := range hosts {
		client, err := newClient(
			observer,
			beat,
			config.Codec,
			host,
			amqpTlsConfig,
			config.DialTimeout,
			exchangeSelector,
			config.ExchangeDeclare,
			routingKeySelector,
			config.PersistentDeliveryMode,
			config.ContentType,
			config.MandatoryPublish,
			config.ImmediatePublish,
			config.EventPrepareConcurrency,
			config.PendingPublishBufferSize,
			config.ChannelMax,
			config.FrameSize,
			config.Heartbeat,
		)

		if err != nil {
			return outputs.Fail(fmt.Errorf("client: %v", err))
		}

		clients[i] = client
	}

	return outputs.SuccessNet(false, config.BulkMaxSize, config.MaxRetries, clients)
}
