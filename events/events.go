package events

import (
	"context"
	"sync"
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
	lock        sync.RWMutex
}

func (e *EventEmitter) allEventSubscription() []*eventSubscription {
	e.lock.RLock()
	defer e.lock.RUnlock()

	return e.Subscribers
}

func (e *EventEmitter) UnsubscribeAll() {
	for _, c := range e.allEventSubscription() {
		c.Cancel()
	}
}

func (e *EventEmitter) Emit(evt Event) {
	for _, s := range e.allEventSubscription() {
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

	e.lock.Lock()
	e.Subscribers = append(e.Subscribers, sub)
	e.lock.Unlock()

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
	e.lock.Lock()
	defer e.lock.Unlock()

	subs := append([]*eventSubscription(nil), e.Subscribers...)

	for i, s := range subs {
		if s == c {
			c.Cancel()

			subs[i] = subs[len(subs)-1]
			e.Subscribers = subs[:len(subs)-1]

			return
		}
	}
}

var _ EmitterInterface = &EventEmitter{}
