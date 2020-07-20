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
	"errors"
	"runtime"
	"time"

	"github.com/elastic/beats/libbeat/common/transport/tlscommon"
	"github.com/elastic/beats/libbeat/outputs/codec"
)

var (
	ErrNoHostsConfigured = errors.New("no hosts configured")
)

type amqpConfig struct {
	Hosts                   []string              `config:"hosts" validate:"required"`
	TLS                     *tlscommon.Config     `config:"ssl"`
	ExchangeDeclare         exchangeDeclareConfig `config:"exchange_declare"`
	PersistentDeliveryMode  bool                  `config:"persistent_delivery_mode"`
	ContentType             string                `config:"content_type"`
	HeadersKey              string                `config:"headers_key"`
	MandatoryPublish        bool                  `config:"mandatory_publish"`
	ImmediatePublish        bool                  `config:"immediate_publish"`
	BulkMaxSize             int                   `config:"bulk_max_size" validate:"min=1"`
	MaxRetries              int                   `config:"max_retries" validate:"min=-1,nonzero"`
	EventPrepareConcurrency uint64                `config:"event_prepare_concurrency" validate:"min=1"`
	ChannelMax              int                   `config:"channel_max" validate:"min=0"`
	FrameSize               int                   `config:"frame_size" validate:"min=0"`
	DialTimeout             time.Duration         `config:"dial_timeout" validate:"min=0"`
	Heartbeat               time.Duration         `config:"heartbeat" validate:"min=0"`
	Codec                   codec.Config          `config:"codec"`
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
		Hosts:                   nil,
		MaxRetries:              3,
		BulkMaxSize:             2048,
		EventPrepareConcurrency: uint64(runtime.GOMAXPROCS(-1)),
		DialTimeout:             30 * time.Second, // same value as amqp library but it's not exported from there
		Heartbeat:               10 * time.Second, // same value as amqp library but it's not exported from there
	}
}

func (c *amqpConfig) Validate() error {
	if len(c.Hosts) == 0 {
		return ErrNoHostsConfigured
	}

	return nil
}
