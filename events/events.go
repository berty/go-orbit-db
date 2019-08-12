// events defines an event subscriber and dispatcher
package events

import (
	"context"
)

// Event Is a base interface for events
type Event interface{}

// EmitterInterface Root interface for events dispatch
type EmitterInterface interface {
	// Emit Sends an event to the subscribed listeners
	Emit(Event)

	// Subscribe Registers a callback that will receive emitted events
	Subscribe(context.Context, func(Event))

	// UnsubscribeAll removes all listeners
	UnsubscribeAll()
}

type eventSubscription struct {
	Chan   chan Event
	Cancel context.CancelFunc
}

// EventEmitter Registers listeners and dispatches events to them
type EventEmitter struct {
	Subscribers []*eventSubscription
}

func (e *EventEmitter) UnsubscribeAll() {
	oldSubscribers := e.Subscribers
	e.Subscribers = nil
	for _, c := range oldSubscribers {
		c.Cancel()
	}
}

func (e *EventEmitter) Emit(evt Event) {
	for _, s := range e.Subscribers {
		select {
		case s.Chan <- evt:
			break
		default:
			break
		}
	}
}

func (e *EventEmitter) Subscribe(ctx context.Context, handler func(Event)) {
	ctx, cancelFunc := context.WithCancel(ctx)

	ch := make(chan Event, 50)

	sub := &eventSubscription{
		Chan:   ch,
		Cancel: cancelFunc,
	}

	e.Subscribers = append(e.Subscribers, sub)

	for {
		select {
		case <-ctx.Done():
			e.unsubscribe(sub)
			return

		case evt := <-ch:
			handler(evt)
		}
	}
}

func (e *EventEmitter) unsubscribe(c *eventSubscription) {
	for i, s := range e.Subscribers {
		if s == c {
			c.Cancel()
			e.Subscribers[len(e.Subscribers)-1], e.Subscribers[i] = e.Subscribers[i], e.Subscribers[len(e.Subscribers)-1]
			e.Subscribers = e.Subscribers[:len(e.Subscribers)-1]
			return
		}
	}
}

var _ EmitterInterface = &EventEmitter{}
