package events

import (
	"container/list"
	"context"
	"fmt"
	"sync"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
)

type Event interface{}

// EmitterInterface Root interface for events dispatch
type EmitterInterface interface {
	// Deprecated: Emit Sends an event to the subscribed listeners
	Emit(context.Context, Event)

	// Deprecated: GlobalChannel returns a glocal channel that receives emitted events
	GlobalChannel(ctx context.Context) <-chan Event

	// Deprecated: Subscribe Returns a channel that receives emitted events
	Subscribe(ctx context.Context) <-chan Event

	// Deprecated: UnsubscribeAll close all listeners channels
	UnsubscribeAll()
}

// Deprecated: use event bus directly
// EventEmitter Registers listeners and dispatches events to them
type EventEmitter struct {
	bus event.Bus

	muEmitters sync.Mutex

	cglobal <-chan Event

	emitter event.Emitter
	cancels []context.CancelFunc
}

type eventBox struct {
	evt interface{}
}

// Deprecated: use event bus directly
// Emit Sends an event to the subscribed listeners
func (e *EventEmitter) Emit(_ context.Context, evt Event) {
	e.muEmitters.Lock()

	bus := e.getBus()
	if e.emitter == nil {
		var err error
		e.emitter, err = bus.Emitter(new(eventBox))
		if err != nil {
			serr := fmt.Sprintf("unable to init eventBox emitter: %s", err.Error())
			panic(serr)
		}
	}

	e.muEmitters.Unlock()
	box := eventBox{evt}
	_ = e.emitter.Emit(box)
}

// Deprecated: use event Bus directly
// Subscribe Returns a channel that receives emitted events
func (e *EventEmitter) Subscribe(ctx context.Context) <-chan Event {
	e.muEmitters.Lock()

	bus := e.getBus()

	sub, err := bus.Subscribe(event.WildcardSubscription)
	if err != nil {
		serr := fmt.Sprintf("unable to subscribe: %s", err.Error())
		panic(serr)
	}

	ctx, cancel := context.WithCancel(ctx)
	if e.cancels == nil {
		e.cancels = []context.CancelFunc{}
	}
	e.cancels = append(e.cancels, cancel)

	e.muEmitters.Unlock()

	return e.handleSubscriber(ctx, sub)
}

// Deprecated: use event bus directly
// UnsubscribeAll close all listeners channels
func (e *EventEmitter) UnsubscribeAll() {
	e.muEmitters.Lock()
	for _, cancel := range e.cancels {
		cancel()
	}

	e.muEmitters.Unlock()
}

func (e *EventEmitter) handleSubscriber(ctx context.Context, sub event.Subscription) <-chan Event {
	cevent := make(chan Event, 16)
	condProcess := sync.NewCond(&sync.Mutex{})
	queue := list.New()
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer sub.Close()

		for {
			var e interface{}
			select {
			case e = <-sub.Out():
			case <-ctx.Done():
				condProcess.Signal()
				return
			}

			if box, ok := e.(eventBox); ok {
				e = box.evt
			}

			condProcess.L.Lock()
			if queue.Len() == 0 {
				// try to push event to the queue
				select {
				case cevent <- e:
					condProcess.L.Unlock()
					continue
				default:
				}
			}

			// push elem to the queue if the channel is blocking or
			// we already have some events to process
			queue.PushBack(e)
			// signal that we have element to process
			condProcess.Signal()
			condProcess.L.Unlock()

		}
	}()

	go func() {
		condProcess.L.Lock()
		for ctx.Err() == nil {
			if queue.Len() == 0 {
				condProcess.Wait()
				continue
			}

			e := queue.Remove(queue.Front())

			// Unlock cond mutex while sending the event
			condProcess.L.Unlock()

			select {
			case <-ctx.Done():
			case cevent <- e:
			}

			condProcess.L.Lock()
		}
		condProcess.L.Unlock()

		wg.Wait()
		close(cevent)
	}()

	return cevent
}

// Deprecated: use event bus directly
// GlobalChannel returns a glocal channel that receives emitted events
func (e *EventEmitter) GlobalChannel(ctx context.Context) (cc <-chan Event) {
	e.muEmitters.Lock()
	if e.cglobal == nil {
		bus := e.getBus()

		sub, err := bus.Subscribe(event.WildcardSubscription)
		if err != nil {
			serr := fmt.Sprintf("unable to subscribe: %s", err.Error())
			panic(serr)
		}

		ctx, cancel := context.WithCancel(ctx)
		if e.cancels == nil {
			e.cancels = []context.CancelFunc{}
		}
		e.cancels = append(e.cancels, cancel)

		e.cglobal = e.handleSubscriber(ctx, sub)
	}

	cc = e.cglobal
	e.muEmitters.Unlock()

	return
}

func (e *EventEmitter) GetBus() (bus event.Bus) {
	e.muEmitters.Lock()
	bus = e.getBus()
	e.muEmitters.Unlock()
	return
}

// set event bus, return an error if the bus is already set
func (e *EventEmitter) SetBus(bus event.Bus) (err error) {
	e.muEmitters.Lock()

	if e.bus == nil {
		e.bus = bus
	} else {
		err = fmt.Errorf("bus is already init")
	}

	e.muEmitters.Unlock()
	return
}

func (e *EventEmitter) getBus() (bus event.Bus) {
	if e.bus == nil {
		e.bus = eventbus.NewBus()
	}

	return e.bus
}
