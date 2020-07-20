// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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

	if config.BulkMaxSize < 1 {
		return outputs.Fail(fmt.Errorf("bulk max size %d must be >= 1", config.BulkMaxSize))
	}
	publishBuffersSize := uint64(config.BulkMaxSize)

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
			config.HeadersKey,
			config.MandatoryPublish,
			config.ImmediatePublish,
			config.EventPrepareConcurrency,
			publishBuffersSize,
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
