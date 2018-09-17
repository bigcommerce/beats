package amqp

import "github.com/elastic/beats/libbeat/publisher"

func newEventTracker(batchTrack *batchTracker, event publisher.Event) eventTracker {
	return eventTracker{
		batchTracker: batchTrack,
		event:        event,
	}
}

type eventTracker struct {
	event        publisher.Event
	batchTracker *batchTracker
}
