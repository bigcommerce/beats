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

package udp

import (
	"time"

	b "github.com/elastic/beats/libbeat/common/backoff"
	"github.com/elastic/beats/libbeat/publisher"
)

type backoffClient struct {
	client *udpOutput

	done    chan struct{}
	backoff b.Backoff
}

func newBackoffClient(client *udpOutput, init, max time.Duration) *backoffClient {
	done := make(chan struct{})
	backoff := b.NewEqualJitterBackoff(done, init, max)
	return &backoffClient{
		client:  client,
		done:    done,
		backoff: backoff,
	}
}

func (b *backoffClient) Connect() error {
	err := b.client.Connect()
	if err != nil {
		// give the client a chance to promote an internal error to a network error.
		b.backoff.Wait()
	}

	return err
}

func (b *backoffClient) Close() error {
	err := b.client.Close()
	close(b.done)
	return err
}

func (b *backoffClient) Publish(batch publisher.Batch) error {
	err := b.client.Publish(batch)
	if err != nil {
		b.client.Close()
		b.backoff.Wait()
	} else {
		b.backoff.Reset()
	}

	return err
}

func (b *backoffClient) String() string {
	return b.client.String()
}
