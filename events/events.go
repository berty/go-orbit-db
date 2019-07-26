package events

type Event interface {}

type EmitterInterface interface {
	Emit(Event)
	Subscribe() chan Event
	Unsubscribe(chan Event)
	UnsubscribeAll()
}

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
	return e.SubscribeWithChan(make(chan Event))
}

func (e *EventEmitter) SubscribeWithChan(c chan Event) chan Event {
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
