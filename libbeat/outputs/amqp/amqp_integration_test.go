// +build integration

package amqp

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/satori/go.uuid"

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
	amqpDefaultURL = "amqp://localhost:5672"
	messageField   = "message"
)

type eventInfo struct {
	events []beat.Event
}

// TODO: separate tests for unhappy things like mandatory=>unroutable, immediate=>noconsumers, ...

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
		title      string
		config     map[string]interface{}
		exchange   string
		routingKey string
		events     []eventInfo
	}{
		{
			"single event",
			nil,
			testExchange,
			testRoutingKey,
			single(common.MapStr{
				messageField: id,
			}),
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
		},
		{
			"batch publish",
			nil,
			testExchange,
			testRoutingKey,
			randMulti(5, 100, common.MapStr{}),
		},
		{
			"batch publish to selected exchange",
			map[string]interface{}{
				"exchange": "%{[foo]}",
			},
			testExchange + "-select",
			testRoutingKey,
			randMulti(5, 100, common.MapStr{
				"foo": testExchange + "-select",
			}),
		},
		{
			"batch publish to selected routing key",
			map[string]interface{}{
				"routing_key": "%{[foo]}",
			},
			testExchange,
			testRoutingKey + "-select",
			randMulti(5, 100, common.MapStr{
				"foo": testRoutingKey + "-select",
			}),
		},
	}

	defaultConfig := map[string]interface{}{
		"hosts":                       []string{getTestAMQPURL()},
		"exchange":                    testExchange,
		"routing_key":                 testRoutingKey,
		"event_prepare_concurrency":   runtime.GOMAXPROCS(-1),
		"pending_publish_buffer_size": 2048,
		"max_publish_attempts":        3,
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
			grp, err := makeAMQP(beat.Info{Beat: "libbeat"}, outputs.NewNilObserver(), cfg)
			if err != nil {
				t.Fatalf("makeAMQP: %v", err)
			}

			output := grp.Clients[0].(*client)
			if err := output.Connect(); err != nil {
				t.Fatal(err)
			}
			defer closeIfNotNil(t, "output:", output)

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

			defer closeIfNotNil(t, "consume: connection close:", consumerConnection)
			defer closeIfNotNil(t, "consume: channel close:", consumerChannel)
			if err != nil {
				t.Fatalf("consume: %v", err)
			}

			// publish event batches

			for _, eventInfo := range test.events {
				batchesWaitGroup.Add(1)
				batch := outest.NewBatch(eventInfo.events...)
				batch.OnSignal = func(_ outest.BatchSignal) {
					batchesWaitGroup.Done()
				}
				output.Publish(batch)
			}

			batchesWaitGroup.Wait()

			// check amqp for the events we published

			consumerTimeout := 2 * time.Second
			deliveries, consumerIdle, consumerClosed := consumeUntilTimeout(deliveriesChan, time.NewTimer(consumerTimeout))
			t.Logf("consumer finished after %v, consumer idled for at least %v", consumerTimeout, consumerIdle)
			if consumerClosed {
				t.Logf("WARN: consumer channel closed before timeout")
			}

			//////

			flattenedMessages := countTestEvents(test.events)
			flattenedDeliveries := countDeliveries(t, deliveries)

			assert.Lenf(t, flattenedDeliveries, len(flattenedMessages), "delivered message count differs from test message count")

			for message, count := range flattenedMessages {
				deliveredCount, _ := flattenedDeliveries[message]
				assert.Equalf(t, count, deliveredCount, "mismatch in count for message, test count: %v, delivered count: %v, message: %v", count, deliveredCount, message)
			}
		})
	}
}

func closeIfNotNil(t *testing.T, prefix string, c io.Closer) {
	if c != nil {
		if err := c.Close(); err != nil {
			t.Logf(prefix+" %v", err)
		}
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
		return nil, nil, nil, fmt.Errorf("consumer: dial: %v", err)
	}
	go logErrors(t, "consumer: connection error: ", connection.NotifyClose(make(chan *amqp.Error)))

	channel, err := connection.Channel()
	if err != nil {
		return nil, connection, nil, fmt.Errorf("consumer: channel: %v", err)
	}
	go logErrors(t, "consumer: channel error: ", channel.NotifyClose(make(chan *amqp.Error)))

	_, err = channel.QueueDeclare(queue, false, true, false, false, nil)
	if err != nil {
		return nil, connection, channel, fmt.Errorf("consumer: queue declare: %v", err)
	}

	err = channel.ExchangeDeclare(exchange, kind, exchangeDurable, exchangeAutoDelete, exchangeInternal, false, nil)
	if err != nil {
		return nil, connection, channel, fmt.Errorf("consumer: exchange declare: %v", err)
	}

	channel.QueueBind(binding, key, exchange, false, nil)
	if err != nil {
		return nil, connection, channel, fmt.Errorf("consumer: queue bind: %v", err)
	}

	deliveries, err := channel.Consume(queue, consumer, true, false, false, false, nil)
	if err != nil {
		return nil, connection, channel, fmt.Errorf("consumer: consume: %v", err)
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

func consumeUntilTimeout(ch <-chan amqp.Delivery, t *time.Timer) (deliveries []amqp.Delivery, idleTime time.Duration, closed bool) {
	lastReceive := time.Now()

	for {
		select {
		case <-t.C:
			idleTime = time.Since(lastReceive)
			return
		case delivery, ok := <-ch:
			if !ok {
				closed = true
				idleTime = time.Since(lastReceive)
				return
			}
			deliveries = append(deliveries, delivery)
			lastReceive = time.Now()
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
			tmp[messageField] = uuid.NewV4().String()
			data = append(data, beat.Event{
				Timestamp: time.Now(),
				Fields:    tmp,
			})
		}

		out = append(out, eventInfo{data})
	}
	return out
}

// common helpers used by unit+integration tests

func randString(length int) string {
	return string(randASCIIBytes(length))
}

func randASCIIBytes(length int) []byte {
	b := make([]byte, length)
	for i := range b {
		b[i] = randChar()
	}
	return b
}

func randChar() byte {
	start, end := 'a', 'z'
	if rand.Int31n(2) == 1 {
		start, end = 'A', 'Z'
	}
	return byte(rand.Int31n(end-start+1) + start)
}
