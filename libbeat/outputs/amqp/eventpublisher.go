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
	"github.com/streadway/amqp"

	"github.com/elastic/beats/libbeat/logp"
)

// newEventPublisher creates a new publisher for sending and confirming prepared
// events using the provided AMQP channel.
//
// The provided AMQP channel MUST NOT have had any previous publishes. Publishes
// will fail if the delivery tag starts at a value other than 1.
//
// The publisher will try and put the provided channel into confirm mode. An
// error will be returned if this fails.
func newEventPublisher(logger *logp.Logger, channel amqpChannel, declarer exchangeDeclarer, preparedEvents <-chan preparedEvent, publishBuffersSize uint64, mandatory, immediate bool) (*eventPublisher, error) {
	if err := channel.Confirm(false); err != nil {
		return nil, err
	}

	ep := &eventPublisher{
		logger:         logger,
		channel:        channel,
		preparedEvents: preparedEvents,
		mandatory:      mandatory,
		immediate:      immediate,
		declarer:       declarer,

		exchanges:        map[string]empty{},
		pendingChan:      make(chan pendingPublish, publishBuffersSize),
		doneChan:         make(chan error, 1),
		confirmationChan: channel.NotifyPublish(make(chan amqp.Confirmation, publishBuffersSize)),
		returnChan:       channel.NotifyReturn(make(chan amqp.Return)),
	}

	ep.logger.Debugf("eventPublisher starting")

	go ep.confirmWorker()
	go ep.publishWorker()

	return ep, nil
}

type eventPublisher struct {
	logger         *logp.Logger
	channel        amqpChannel
	preparedEvents <-chan preparedEvent
	mandatory      bool
	immediate      bool
	declarer       exchangeDeclarer

	exchanges        map[string]empty
	pendingChan      chan pendingPublish
	doneChan         chan error
	confirmationChan chan amqp.Confirmation
	returnChan       chan amqp.Return
	deliveryTag      uint64
}

// done blocks until eventPublisher has finished. The first error found on the
// internal doneChan will be returned, if any.
func (e *eventPublisher) done() (err error) {
	defer e.logger.Debugf("eventPublisher finished")
	for e := range e.doneChan {
		if e != nil && err == nil {
			err = e
		}
	}
	return
}

func (e *eventPublisher) ensureDeclared(exchange string) error {
	if _, declared := e.exchanges[exchange]; !declared {
		if err := e.declarer(e.channel, exchange); err != nil {
			return err
		}
		e.exchanges[exchange] = empty{}
	}
	return nil
}

func (e *eventPublisher) confirmWorker() {
	defer close(e.doneChan)
	e.logger.Debugf("confirmWorker starting")

	var errLogged bool
	var pendingCounter, retryCounter, confirmCounter uint64
	for pending := range e.pendingChan {
		pendingCounter++

		_, ret, err := getNextConfirmation(e.logger, pending, e.returnChan, e.confirmationChan)

		if err != nil {
			// drain and retry everything on pendingChan, but only log once
			if !errLogged {
				errLogged = true
				e.logger.Errorf("AMQP confirmation error, remaining pending publishes which fail like this will be considered NACKed and retried: %v", err)
				e.doneChan <- err
			}
			retryCounter++
			pending.retry()
			continue
		}

		if ret != nil {
			e.logger.Warnf("AMQP returned message, will retry, reply: %v (%v)", ret.ReplyText, ret.ReplyCode)
			retryCounter++
			pending.retry()
			continue
		}

		confirmCounter++
		pending.confirm()
	}

	e.logger.Debugf("confirmWorker finished, processed %v pending publishes, retries: %v, confirms: %v", pendingCounter, retryCounter, confirmCounter)
}

// publishWorker attempts to publish the contents of preparedEvents to the
// current AMQP channel.
//
// publishWorker ends when preparedEvents is closed, when declaring an exchange
// fails, or when channel.Publish fails.
func (e *eventPublisher) publishWorker() {
	// Note: Let confirmWorker close doneChan. It should close it shortly after
	// pendingChan is closed, which will happen here if there's an error.
	defer close(e.pendingChan)
	defer e.logger.Debugf("publishWorker finished")
	e.logger.Debugf("publishWorker starting")

	for event := range e.preparedEvents {
		if err := e.ensureDeclared(event.exchangeName); err != nil {
			// Errors returned from exchange declare are meant to close the AMQP
			// channel, so we shut down the publisher since we won't be able to
			// publish future events.
			event.incomingEvent.batchTracker.retryEvent(event.incomingEvent.event)
			e.logger.Errorf("AMQP exchange declare error: %v", err)
			e.doneChan <- err
			return
		}

		if err := e.channel.Publish(
			event.exchangeName,
			event.routingKey,
			e.mandatory,
			e.immediate,
			event.outgoingPublishing,
		); err != nil {
			// Publish is asynchronous, so the assumption here is that a Publish
			// error is a result of a connection issue. So we shut down the
			// publisher, but only after signalling to the batch that the event
			// should be retried (since it's no longer on the preparedEvents
			// channel).
			event.incomingEvent.batchTracker.retryEvent(event.incomingEvent.event)
			e.logger.Errorf("AMQP publish error: %v", err)
			e.doneChan <- err
			return
		}

		e.deliveryTag++
		e.pendingChan <- pendingPublish{
			event:       event,
			deliveryTag: e.deliveryTag,
		}
	}
}
