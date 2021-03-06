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

// +build integration

package amqp

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/outest"

	_ "github.com/elastic/beats/libbeat/outputs/codec/format"
	_ "github.com/elastic/beats/libbeat/outputs/codec/json"
)

const (
	amqpDefaultURL    = "amqp://localhost:5672"
	messageField      = "message"
	defaultBatchCount = 5
	defaultBatchSize  = 100
)

type eventInfo struct {
	events []beat.Event
}

func TestAMQPRetryOnDialError(t *testing.T) {
	logp.TestingSetup(logp.WithSelectors("amqp"))

	id := strconv.Itoa(rand.New(rand.NewSource(int64(time.Now().Nanosecond()))).Int())

	defaultConfig := map[string]interface{}{
		"hosts":       []string{"amqp://foo.invalid:5672"},
		"exchange":    fmt.Sprintf("test-libbeat-%s", id),
		"routing_key": fmt.Sprintf("test-libbeat-%s", id),
		"exchange_declare": map[string]interface{}{
			"enabled":     true,
			"kind":        "direct",
			"auto_delete": true,
		},
	}

	cfg := makeConfig(t, defaultConfig)

	grp, err := makeAMQP(nil, beat.Info{Beat: "libbeat"}, outputs.NewNilObserver(), cfg)
	if err != nil {
		t.Fatalf("makeAMQP: %v", err)
	}

	output := grp.Clients[0].(*client)

	expectedDials := 3
	dials := 0
	dialWG := &sync.WaitGroup{}
	dialWG.Add(expectedDials)
	output.dialFunc = func(network, addr string) (net.Conn, error) {
		fmt.Printf("mock dial called\n")
		dials++
		dialWG.Done()
		return nil, fmt.Errorf("mock dial failure")
	}

	if err := output.Connect(); err != nil {
		t.Fatal(err)
	}
	defer checkClose(t, "output", output)

	batch := outest.NewBatch(single(common.MapStr{
		messageField: id,
	})[0].events...)

	batch.OnSignal = func(signal outest.BatchSignal) {
		// defer batchesWaitGroup.Done()
		// if signal.Tag == outest.BatchACK {
		// 	t.Logf("batch %v ACKed", batchId)
		// } else {
		// 	t.Errorf("batch %v was not ACKed, actual signal was: %v, returned events len: %v", batchId, signal.Tag, len(signal.Events))
		// }
		t.Logf("batch signal: %#v", signal)
	}

	if err := output.Publish(batch); err != nil {
		t.Fatal(err)
	}

	dialWG.Wait()
	assert.Equal(t, expectedDials, dials)
}

func TestAMQPPublish(t *testing.T) {
	logp.TestingSetup(logp.WithSelectors("amqp"))

	id := strconv.Itoa(rand.New(rand.NewSource(int64(time.Now().Nanosecond()))).Int())
	testExchange := fmt.Sprintf("test-libbeat-%s", id)
	testExchangeKind := "direct"
	testRoutingKey := fmt.Sprintf("test-libbeat-%s", id)
	testQueue := fmt.Sprintf("test-libbeat-%s", id)
	testConsumer := fmt.Sprintf("test-libbeat-%s", id)
	testBinding := fmt.Sprintf("test-libbeat-%s", id)

	tests := []struct {
		title           string
		config          map[string]interface{}
		exchange        string
		routingKey      string
		events          []eventInfo
		expectedHeaders amqp.Table
		// expectedTotalDeliveries specifies the expected number of messages delivered,
		// set to -1 if all messages are expected to be delivered
		expectedTotalDeliveries int
	}{
		{
			"single event",
			nil,
			testExchange,
			testRoutingKey,
			single(common.MapStr{
				messageField: id,
			}),
			nil,
			-1,
		},
		{
			"single event to selected exchange",
			map[string]interface{}{
				"exchange": "%{[foo]}",
			},
			testExchange + "-select",
			testRoutingKey,
			single(common.MapStr{
				"foo":        testExchange + "-select",
				messageField: id,
			}),
			nil,
			-1,
		},
		{
			"single event to selected routing key",
			map[string]interface{}{
				"routing_key": "%{[foo]}",
			},
			testExchange,
			testRoutingKey + "-select",
			single(common.MapStr{
				"foo":        testRoutingKey + "-select",
				messageField: id,
			}),
			nil,
			-1,
		},
		{
			"batch publish",
			nil,
			testExchange,
			testRoutingKey,
			randMulti(defaultBatchCount, defaultBatchSize, common.MapStr{}),
			nil,
			-1,
		},
		{
			"batch publish to selected exchange",
			map[string]interface{}{
				"exchange": "%{[foo]}",
			},
			testExchange + "-select",
			testRoutingKey,
			randMulti(defaultBatchCount, defaultBatchSize, common.MapStr{
				"foo": testExchange + "-select",
			}),
			nil,
			-1,
		},
		{
			"batch publish to selected routing key",
			map[string]interface{}{
				"routing_key": "%{[foo]}",
			},
			testExchange,
			testRoutingKey + "-select",
			randMulti(defaultBatchCount, defaultBatchSize, common.MapStr{
				"foo": testRoutingKey + "-select",
			}),
			nil,
			-1,
		},
		{
			"single event to selected exchange with headers",
			map[string]interface{}{
				"exchange":    "%{[foo]}",
				"headers_key": "headers",
			},
			testExchange + "-select",
			testRoutingKey,
			single(common.MapStr{
				"foo": testExchange + "-select",
				"headers": common.MapStr{
					"content-type": "application/grpc+json",
				},
				messageField: id,
			}),
			amqp.Table{
				"content-type": "application/grpc+json",
			},
			-1,
		},
		{
			"single event to selected exchange with missing headers",
			map[string]interface{}{
				"exchange":    "%{[foo]}",
				"headers_key": "headers",
			},
			testExchange + "-select",
			testRoutingKey,
			single(common.MapStr{
				"foo":        testExchange + "-select",
				messageField: id,
			}),
			nil,
			-1,
		},
		{
			"single event to selected exchange ignoring headers",
			map[string]interface{}{
				"exchange": "%{[foo]}",
			},
			testExchange + "-select",
			testRoutingKey,
			single(common.MapStr{
				"foo": testExchange + "-select",
				// headers_key not configured, so headers will be ignored
				"headers": common.MapStr{
					"content-type": "application/grpc+json",
				},
				messageField: id,
			}),
			nil,
			-1,
		},
		{
			"batch with invalid events dropped",
			map[string]interface{}{
				"exchange":    "%{[exchange]}",
				"routing_key": "%{[routing_key]}",
				"codec.format": map[string]string{
					"string": "%{[message]}",
				},
			},
			testExchange + "-select",
			testRoutingKey + "-select",
			[]eventInfo{
				{
					events: []beat.Event{
						{
							Timestamp: time.Now(),
							Fields: common.MapStr{
								"exchange":    testExchange + "-select",
								"routing_key": testRoutingKey + "-select",
								messageField:  "{\"message\": \"hello\"}",
							},
						},
						{
							Timestamp: time.Now(),
							Fields: common.MapStr{
								"routing_key": testRoutingKey + "-select",
								"error":       "Error decoding JSON",
							},
						},
						{
							Timestamp: time.Now(),
							Fields: common.MapStr{
								"exchange": testExchange + "-select",
								"error":    "Error decoding JSON",
							},
						},
						{
							Timestamp: time.Now(),
							Fields: common.MapStr{
								"exchange":    testExchange + "-select",
								"routing_key": testRoutingKey + "-select",
								messageField:  "{\"message\": \"hello\"}",
							},
						},
					},
				},
			},
			nil,
			2, // Expecting the two valid events to be published only
		},
	}

	defaultConfig := map[string]interface{}{
		"hosts":       []string{getTestAMQPURL()},
		"exchange":    testExchange,
		"routing_key": testRoutingKey,
		"exchange_declare": map[string]interface{}{
			"enabled":     true,
			"kind":        testExchangeKind,
			"auto_delete": true,
		},
	}

	for i, test := range tests {
		test := test
		name := fmt.Sprintf("run test(%v): %v", i, test.title)

		cfg := makeConfig(t, defaultConfig)
		if test.config != nil {
			cfg.Merge(makeConfig(t, test.config))
		}

		t.Run(name, func(t *testing.T) {
			grp, err := makeAMQP(nil, beat.Info{Beat: "libbeat"}, outputs.NewNilObserver(), cfg)
			if err != nil {
				t.Errorf("makeAMQP: %v", err)
				return
			}

			output := grp.Clients[0].(*client)
			if err := output.Connect(); err != nil {
				t.Errorf("output.Connect: %v", err)
				return
			}
			defer checkClose(t, "output", output)

			batchesWaitGroup := &sync.WaitGroup{}

			// begin consuming from amqp ahead of sending events to cater for
			// various persistence configurations

			deliveriesChan, consumerConnection, consumerChannel, err := testConsume(
				t,
				getTestAMQPURL(),
				test.exchange,
				testExchangeKind,
				testBinding,
				testQueue,
				testConsumer,
				test.routingKey,
			)

			if consumerConnection != nil {
				defer checkClose(t, "consume connection", consumerConnection)
			}

			if consumerChannel != nil {
				defer checkClose(t, "consume channel", consumerChannel)
			}

			if err != nil {
				fmt.Printf("testConsume(%v): %v\n", name, err)
				t.Errorf("consume: %v", err)
				return
			}

			// publish event batches
			t.Logf("publishing %v batches", len(test.events))
			for batchId, eventInfo := range test.events {
				batchId := batchId
				batchesWaitGroup.Add(1)
				batch := outest.NewBatch(eventInfo.events...)
				batch.OnSignal = func(signal outest.BatchSignal) {
					defer batchesWaitGroup.Done()
					if signal.Tag == outest.BatchACK {
						t.Logf("batch %v ACKed", batchId)
					} else {
						t.Errorf("batch %v was not ACKed, actual signal was: %v, returned events len: %v", batchId, signal.Tag, len(signal.Events))
					}
				}
				output.Publish(batch)
			}

			t.Logf("waiting for batches to publish")
			batchesWaitGroup.Wait()

			// check amqp for the events we published
			t.Logf("consuming events from AMQP")
			consumerTimeout := 3 * time.Second
			deliveries, consumerClosed := consumeUntilTimeout(deliveriesChan, consumerTimeout)
			if consumerClosed {
				t.Logf("WARN: consumer channel closed before timeout, which may indicate a connectivity issue")
			} else {
				t.Logf("stopped consuming after %v timeout", consumerTimeout)
			}

			// Verify the number of messages delivered
			if test.expectedTotalDeliveries != -1 {
				deliveredCount := len(deliveries)
				assert.Equalf(t, test.expectedTotalDeliveries, deliveredCount, "mismatch in count of deliveries , want: %v, delivered count: %v,", test.expectedTotalDeliveries, deliveredCount)
				return
			}

			//////

			flattenedMessages := countTestEvents(test.events)
			flattenedDeliveries := countDeliveries(t, deliveries)

			assert.Lenf(t, flattenedDeliveries, len(flattenedMessages), "delivered message count differs from test message count")

			for message, count := range flattenedMessages {
				deliveredCount, _ := flattenedDeliveries[message]
				assert.Equalf(t, count, deliveredCount, "mismatch in count for message, test count: %v, delivered count: %v, message: %v", count, deliveredCount, message)
			}

			for _, delivery := range deliveries {
				assert.Equalf(t, test.expectedHeaders, delivery.Headers, "mismatched headers for message, expected: %v,  delivered: %v", test.expectedHeaders, delivery.Headers)
			}
		})
	}
}

func TestAMQPRetry(t *testing.T) {
	logp.TestingSetup(logp.WithSelectors("amqp"))

	id := strconv.Itoa(rand.New(rand.NewSource(int64(time.Now().Nanosecond()))).Int())
	testExchange := fmt.Sprintf("test-libbeat-%s", id)
	testRoutingKey := fmt.Sprintf("test-libbeat-%s", id)

	tests := []struct {
		title      string
		config     map[string]interface{}
		exchange   string
		routingKey string
		events     []eventInfo
	}{
		{
			"mandatory batch publish to unroutable key should all be retried",
			map[string]interface{}{
				"mandatory_publish": true,
				"exchange_declare": map[string]interface{}{
					"enabled":     true,
					"kind":        "fanout",
					"auto_delete": true,
				},
			},
			testExchange,
			testRoutingKey + "-missing",
			randMulti(defaultBatchCount, defaultBatchSize, common.MapStr{}),
		},
		{
			"batch publish to missing exchange should all be retried",
			map[string]interface{}{},
			testExchange + "-not-declared",
			testRoutingKey,
			randMulti(defaultBatchCount, defaultBatchSize, common.MapStr{}),
		},
		// TODO: consider testing immediate_publish, however, we'd need a queue-bind setup for that test
	}

	for i, test := range tests {
		test := test
		name := fmt.Sprintf("run test(%v): %v", i, test.title)

		defaultConfig := map[string]interface{}{
			"hosts":       []string{getTestAMQPURL()},
			"exchange":    test.exchange,
			"routing_key": test.routingKey,
		}

		cfg := makeConfig(t, defaultConfig)
		if test.config != nil {
			cfg.Merge(makeConfig(t, test.config))
		}

		t.Run(name, func(t *testing.T) {
			grp, err := makeAMQP(nil, beat.Info{Beat: "libbeat"}, outputs.NewNilObserver(), cfg)
			if err != nil {
				t.Fatalf("makeAMQP: %v", err)
			}

			output := grp.Clients[0].(*client)
			if err := output.Connect(); err != nil {
				t.Fatal(err)
			}
			defer checkClose(t, "output", output)

			// publish event batches
			var signals int64
			wg := &sync.WaitGroup{}
			t.Logf("publishing %v batches", len(test.events))
			for batchId, eventInfo := range test.events {
				batchId := batchId
				wg.Add(1)
				batch := outest.NewBatch(eventInfo.events...)
				batch.OnSignal = func(signal outest.BatchSignal) {
					defer wg.Done()
					assert.Equal(t, outest.BatchRetryEvents, signal.Tag, "unexpected batch signal")
					assert.Equal(t, len(test.events[batchId].events), len(signal.Events), "unexpected size of retry events slice")
					for _, batchEvent := range test.events[batchId].events {
						var found bool
						for _, signalEvent := range signal.Events {
							if signalEvent.Content.Fields[messageField] == batchEvent.Fields[messageField] {
								found = true
								break
							}
						}
						assert.True(t, found, "message in batch not found in retry list")
					}

					atomic.AddInt64(&signals, 1)
				}

				output.Publish(batch)
			}

			t.Logf("waiting for batches to publish")
			wg.Wait()
			t.Logf("done")
			assert.Equal(t, len(test.events), int(signals), "did not receive correct amount of batch signals")
		})
	}
}

func checkClose(t *testing.T, name string, c io.Closer) {
	fmt.Printf("closing %s ...\n", name)
	if err := c.Close(); err != nil {
		fmt.Printf("close %s error: %v\n", name, err)
		t.Errorf("%s close: %v", name, err)
	} else {
		fmt.Printf("%s closed\n", name)
	}
}

func testConsume(t *testing.T, url, exchange, kind, binding, queue, consumer, key string) (<-chan amqp.Delivery, *amqp.Connection, *amqp.Channel, error) {
	const (
		exchangeDurable    = false
		exchangeAutoDelete = true
		exchangeInternal   = false
	)

	connection, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("test consumer: dial: %v", err)
	}
	go logErrors(t, "test consumer: connection error: ", connection.NotifyClose(make(chan *amqp.Error)))

	channel, err := connection.Channel()
	if err != nil {
		return nil, connection, nil, fmt.Errorf("test consumer: channel: %v", err)
	}
	go logErrors(t, "test consumer: channel error: ", channel.NotifyClose(make(chan *amqp.Error)))

	_, err = channel.QueueDeclare(queue, false, true, false, false, nil)
	if err != nil {
		return nil, connection, channel, fmt.Errorf("test consumer: queue declare: %v", err)
	}

	err = channel.ExchangeDeclare(exchange, kind, exchangeDurable, exchangeAutoDelete, exchangeInternal, false, nil)
	if err != nil {
		return nil, connection, channel, fmt.Errorf("test consumer: exchange declare: %v", err)
	}

	channel.QueueBind(binding, key, exchange, false, nil)
	if err != nil {
		return nil, connection, channel, fmt.Errorf("test consumer: queue bind: %v", err)
	}

	deliveries, err := channel.Consume(queue, consumer, true, false, false, false, nil)
	if err != nil {
		return nil, connection, channel, fmt.Errorf("test consumer: consume: %v", err)
	}

	return deliveries, connection, channel, nil
}

func decodeMessage(t *testing.T, body []byte) string {
	var decoded map[string]interface{}
	err := json.Unmarshal(body, &decoded)
	assert.NoError(t, err)
	message, found := decoded[messageField]
	assert.True(t, found)
	str, ok := message.(string)
	assert.True(t, ok)
	return str
}

// consumeUntilTimeout consumes deliveries from ch until no more arrive after a
// timeout period (which resets after each delivery). A slice of consumed
// deliveries is returned, along with a bool flag which will be true if ch was
// closed before a timeout occurred.
func consumeUntilTimeout(ch <-chan amqp.Delivery, timeout time.Duration) (deliveries []amqp.Delivery, closed bool) {
	timer := time.NewTimer(timeout)

	for {
		select {
		case <-timer.C:
			return
		case delivery, ok := <-ch:
			if !timer.Stop() {
				<-timer.C
			}

			if !ok {
				closed = true
				return
			}

			deliveries = append(deliveries, delivery)
			timer.Reset(timeout)
		}
	}
}

func logErrors(t *testing.T, prefix string, ch <-chan *amqp.Error) {
	for err := range ch {
		t.Logf(prefix+"%v", err)
	}
}

func makeConfig(t *testing.T, in map[string]interface{}) *common.Config {
	cfg, err := common.NewConfigFrom(in)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func strDefault(a, defaults string) string {
	if len(a) == 0 {
		return defaults
	}
	return a
}

func getenv(name, defaultValue string) string {
	return strDefault(os.Getenv(name), defaultValue)
}

func getTestAMQPURL() string {
	return getenv("AMQP_URL", amqpDefaultURL)
}

func countTestEvents(infos []eventInfo) map[string]uint64 {
	out := map[string]uint64{}
	for _, info := range infos {
		for _, event := range info.events {
			message := event.Fields[messageField].(string)
			if _, found := out[message]; found {
				out[message]++
			} else {
				out[message] = 1
			}
		}
	}
	return out
}

func countDeliveries(t *testing.T, deliveries []amqp.Delivery) map[string]uint64 {
	out := map[string]uint64{}
	for _, delivery := range deliveries {
		message := decodeMessage(t, delivery.Body)
		if _, found := out[message]; found {
			out[message]++
		} else {
			out[message] = 1
		}
	}
	return out
}

func single(fields common.MapStr) []eventInfo {
	return []eventInfo{
		{
			events: []beat.Event{
				{Timestamp: time.Now(), Fields: fields},
			},
		},
	}
}

func randMulti(batches, n int, event common.MapStr) []eventInfo {
	var out []eventInfo
	for i := 0; i < batches; i++ {
		var data []beat.Event
		for j := 0; j < n; j++ {
			tmp := common.MapStr{}
			for k, v := range event {
				tmp[k] = v
			}
			tmp[messageField] = strconv.Itoa(int(time.Now().UnixNano()))
			data = append(data, beat.Event{
				Timestamp: time.Now(),
				Fields:    tmp,
			})
		}

		out = append(out, eventInfo{data})
	}
	return out
}
