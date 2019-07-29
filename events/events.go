// events defines an event subscriber and dispatcher
package events

type Event interface{}

// EmitterInterface Root interface for events dispatch
type EmitterInterface interface {
	// Emit Sends an event to the subscribed listeners
	Emit(Event)

	// Subscribe Get a channel that will receive emitted events
	Subscribe() chan Event

	// Unsubscribe Unregister a channel
	Unsubscribe(chan Event)

	// UnsubscribeAll removes all listeners
	UnsubscribeAll()
}

// EventEmitter Registers listeners and dispatches events to them
type EventEmitter struct {
	Subscribers []chan Event
}

func (e *EventEmitter) UnsubscribeAll() {
	e.Subscribers = []chan Event{}
}

func (e *EventEmitter) Emit(evt Event) {
	for _, s := range e.Subscribers {
		s <- evt
	}
}

func (e *EventEmitter) Subscribe() chan Event {
	return e.subscribeWithChan(make(chan Event))
}

func (e *EventEmitter) subscribeWithChan(c chan Event) chan Event {
	for _, s := range e.Subscribers {
		if s == c {
			return nil
		}
	}

	e.Subscribers = append(e.Subscribers, c)

	return c
}

func (e *EventEmitter) Unsubscribe(c chan Event) {
	for i, s := range e.Subscribers {
		if s == c {
			e.Subscribers[len(s)-1], e.Subscribers[i] = e.Subscribers[i], e.Subscribers[len(s)-1]
			e.Subscribers = e.Subscribers[:len(s)-1]
			return
		}
	}
}

var _ EmitterInterface = &EventEmitter{}
