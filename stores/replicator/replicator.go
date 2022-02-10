// replicator the replication logic for an OrbitDB store
package replicator

import (
	"context"
	"strings"
	"sync"

	ipfslog "berty.tech/go-ipfs-log"
	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/pkg/errors"
	otkv "go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

var batchSize = 1

type queuedState int

const (
	stateAdded queuedState = iota
	stateFetching
)

type replicator struct {
	eventBus event.Bus
	emitters struct {
		evtLoadEnd      event.Emitter
		evtLoadAdded    event.Emitter
		evtLoadProgress event.Emitter
	}

	taskInProgress int64

	cancelFunc  context.CancelFunc
	store       storeInterface
	concurrency int64

	tasks map[cid.Cid]queuedState
	queue []cid.Cid

	buffer   []ipfslog.Log
	muBuffer sync.Mutex

	muProcess   *sync.RWMutex
	condProcess *sync.Cond

	lock   sync.RWMutex
	logger *zap.Logger
	tracer trace.Tracer
}

type Options struct {
	Logger   *zap.Logger
	Tracer   trace.Tracer
	EventBus event.Bus
}

// NewReplicator Creates a new Replicator instance
func NewReplicator(store storeInterface, concurrency uint, opts *Options) (Replicator, error) {
	if opts == nil {
		opts = &Options{}
	}

	if opts.EventBus == nil {
		opts.EventBus = eventbus.NewBus()
	}

	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}

	if opts.Tracer == nil {
		opts.Tracer = trace.NewNoopTracerProvider().Tracer("")
	}

	if concurrency == 0 {
		concurrency = 32
	}

	muProcess := sync.RWMutex{}
	r := replicator{
		eventBus:    opts.EventBus,
		concurrency: int64(concurrency),
		store:       store,
		tasks:       make(map[cid.Cid]queuedState),
		queue:       []cid.Cid{},
		logger:      opts.Logger,
		tracer:      opts.Tracer,
		muProcess:   &muProcess,
		condProcess: sync.NewCond(&muProcess),
	}
	if err := r.generateEmitter(opts.EventBus); err != nil {
		return nil, err
	}

	return &r, nil
}

func (r *replicator) Stop() {
	emitters := []event.Emitter{
		r.emitters.evtLoadEnd,
		r.emitters.evtLoadAdded,
		r.emitters.evtLoadProgress,
	}

	for _, emitter := range emitters {
		if err := emitter.Close(); err != nil {
			r.logger.Warn("unable to close emitter", zap.Error(err))
		}
	}
}

func (r *replicator) GetQueue() []cid.Cid {
	r.muProcess.Lock()
	defer r.muProcess.Unlock()

	return r.queue
}

func (r *replicator) Load(ctx context.Context, cids []cid.Cid) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cidsStrings := make([]string, len(cids))
	for i, c := range cids {
		cidsStrings[i] = c.String()
	}

	ctx, span := r.tracer.Start(ctx, "replicator-load", trace.WithAttributes(otkv.String("cids", strings.Join(cidsStrings, ","))))
	defer span.End()

	// process and wait the whole queue to complete
	wg := sync.WaitGroup{}

	r.muProcess.Lock()
	for _, h := range cids {
		_, inLog := r.store.OpLog().Get(h)
		_, queued := r.tasks[h]
		if queued || inLog {
			continue
		}

		r.queue = append(r.queue, h)
		r.tasks[h] = stateAdded

		// add one process
		wg.Add(1)
		go func() {
			defer wg.Done()
			next, err := r.processOne(ctx)
			if err != nil {
				r.logger.Error("uanble process cids", zap.Error(err))
				return
			}

			if len(next) > 0 {
				// load next cids
				r.Load(ctx, next)
			}
		}()
	}
	r.muProcess.Unlock()

	wg.Wait()

	// send load end event
	r.muProcess.Lock()
	if r.taskInProgress == 0 {
		// should be thread safe
		// @NOTE(gfanton) we should probably do this when load is done instead
		if len(r.buffer) > 0 {
			if err := r.emitters.evtLoadEnd.Emit(NewEventLoadEnd(r.buffer)); err != nil {
				r.logger.Warn("unable to emit event load end", zap.Error(err))
			}
			r.buffer = []ipfslog.Log{}
		}
	}
	r.muProcess.Unlock()
}

func (r *replicator) waitForProcessSlot(ctx context.Context) (h cid.Cid, err error) {
	r.condProcess.L.Lock()

	r.taskInProgress++
	for r.taskInProgress >= r.concurrency {
		r.condProcess.Wait()
	}

	select {
	case <-ctx.Done():
		r.taskInProgress--
		r.condProcess.Signal()
		err = ctx.Err()
	default:
		// pop hash from queue
		h = r.queue[0]
		r.queue = r.queue[1:]
		r.tasks[h] = stateFetching
	}

	r.condProcess.L.Unlock()
	return
}

func (r *replicator) processDone(h cid.Cid) {
	r.condProcess.L.Lock()

	r.taskInProgress--

	// remove hash from queued list
	delete(r.tasks, h)

	// signal that a process slot is available
	r.condProcess.Signal()

	r.condProcess.L.Unlock()
}

func (r *replicator) processOne(ctx context.Context) ([]cid.Cid, error) {
	h, err := r.waitForProcessSlot(ctx)
	if err != nil {
		return nil, err
	}

	defer r.processDone(h)

	ctx, span := r.tracer.Start(ctx, "replicator-process-one")
	defer span.End()

	entry, hasEntry := r.store.OpLog().Get(h)
	if hasEntry {
		return nil, nil
	}

	if err := r.emitters.evtLoadAdded.Emit(NewEventLoadAdded(h, entry)); err != nil {
		r.logger.Warn("unable to emit event load added", zap.Error(err))
	}

	l, err := ipfslog.NewFromEntryHash(ctx, r.store.IPFS(), r.store.Identity(), h, &ipfslog.LogOptions{
		ID:               r.store.OpLog().GetID(),
		AccessController: r.store.AccessController(),
		SortFn:           r.store.SortFn(),
		IO:               r.store.IO(),
	}, &ipfslog.FetchOptions{
		Length: &batchSize,
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to fetch log")
	}

	latest := l.Values().At(0)

	r.muBuffer.Lock()
	r.buffer = append(r.buffer, l)

	// Notify subscribers that we made progress
	if err := r.emitters.evtLoadProgress.Emit(NewEventLoadProgress("", h, latest, len(r.buffer))); err != nil {
		r.logger.Warn("unable to emit event load progress", zap.Error(err))
	}
	r.muBuffer.Unlock()

	var nextValues []cid.Cid

	for _, e := range l.Values().Slice() {
		nextValues = append(nextValues, e.GetNext()...)
		nextValues = append(nextValues, e.GetRefs()...)
	}

	// Return all next pointers
	return nextValues, nil
}

func (r *replicator) EventBus() event.Bus {
	return r.eventBus
}

func (r *replicator) generateEmitter(bus event.Bus) error {
	var err error

	if r.emitters.evtLoadEnd, err = bus.Emitter(new(EventLoadEnd)); err != nil {
		return errors.Wrap(err, "unable to create EventLoadEnd emitter")
	}

	if r.emitters.evtLoadAdded, err = bus.Emitter(new(EventLoadAdded)); err != nil {
		return errors.Wrap(err, "unable to create EventLoadAdded emitter")
	}

	if r.emitters.evtLoadProgress, err = bus.Emitter(new(EventLoadProgress)); err != nil {
		return errors.Wrap(err, "unable to create EventLoadProgress emitter")
	}

	return nil
}

var _ Replicator = &replicator{}
