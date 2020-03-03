package events

import (
	"context"
	"sync"

	"github.com/golang-collections/go-datastructures/queue"
)

// Event Is a base interface for events
type Event interface{}

// EmitterInterface Root interface for events dispatch
type EmitterInterface interface {
	// Emit Sends an event to the subscribed listeners
	Emit(context.Context, Event)

	// Subscribe Returns a channel that receives emitted events
	Subscribe(context.Context) <-chan Event

	// UnsubscribeAll close all listeners channels
	UnsubscribeAll()
}

// EventEmitter Registers listeners and dispatches events to them
type EventEmitter struct {
	subs []*eventSubscription
	lock sync.Mutex
}

func (e *EventEmitter) UnsubscribeAll() {
	for _, s := range e.allEventSubscription() {
		s.cancel()
	}
}

type eventSubscription struct {
	ch     chan Event
	mu     sync.Mutex
	cancel context.CancelFunc
	ctx    context.Context
	queue  queue.Queue
	closed bool
}

func (e *EventEmitter) allEventSubscription() []*eventSubscription {
	e.lock.Lock()
	defer e.lock.Unlock()

	return append([]*eventSubscription(nil), e.subs...)
}

func (s *eventSubscription) queuedEmit(ctx context.Context) {
	for {
		items, err := s.queue.Get(1)
		if err != nil {
			return
		}

		if len(items) != 1 {
			continue
		}

		evt, ok := items[0].(Event)
		if !ok {
			continue
		}

		s.mu.Lock()
		if ctx.Err() != nil {
			s.mu.Unlock()
			return
		}

		select {
		case <-ctx.Done():
			s.mu.Unlock()
			return

		case s.ch <- evt:
		}

		s.mu.Unlock()
	}
}

func (s *eventSubscription) emit(evt Event) {
	if err := s.queue.Put(evt); err != nil {
		s.close()
	}
}

func (e *EventEmitter) Emit(ctx context.Context, evt Event) {
	for _, sub := range e.allEventSubscription() {
		sub.emit(evt)
	}
}

func (s *eventSubscription) close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}

	s.closed = true

	s.cancel()
	close(s.ch)
}

func (e *EventEmitter) removeSub(s *eventSubscription) {
	e.lock.Lock()
	defer e.lock.Unlock()

	subs := e.subs

	for i, can := range subs {
		if can == s {
			subs[i] = subs[len(subs)-1]
			subs[len(subs)-1] = nil
			e.subs = subs[:len(subs)-1]

			break
		}
	}
}

func (e *EventEmitter) Subscribe(ctx context.Context) <-chan Event {
	sub := &eventSubscription{
		ch: make(chan Event, 0),
	}

	sub.ctx, sub.cancel = context.WithCancel(ctx)

	e.lock.Lock()
	e.subs = append(e.subs, sub)
	e.lock.Unlock()

	go func() {
		<-sub.ctx.Done()
		e.removeSub(sub)
		sub.queue.Dispose()
		sub.close()
	}()

	go sub.queuedEmit(sub.ctx)

	return sub.ch
}

var _ EmitterInterface = &EventEmitter{}
