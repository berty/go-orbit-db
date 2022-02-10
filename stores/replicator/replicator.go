// replicator the replication logic for an OrbitDB store
package replicator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

type replicator struct {
	eventBus event.Bus
	emitters struct {
		evtLoadEnd      event.Emitter
		evtLoadAdded    event.Emitter
		evtLoadProgress event.Emitter
	}

	// These require 64 bit alignment for ARM and 32bit devices
	statsTasksRequested int64
	statsTasksStarted   int64
	statsTasksProcessed int64
	// For more information see https://pkg.go.dev/sync/atomic#pkg-note-BUG

	cancelFunc  context.CancelFunc
	store       storeInterface
	fetching    map[string]cid.Cid
	buffer      []ipfslog.Log
	concurrency int64
	queue       map[string]cid.Cid
	lock        sync.RWMutex
	logger      *zap.Logger
	tracer      trace.Tracer
}

func (r *replicator) GetBufferLen() int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return len(r.buffer)
}

func (r *replicator) Stop() {
	r.cancelFunc()

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
	r.lock.RLock()
	defer r.lock.RUnlock()

	queue := make([]cid.Cid, len(r.queue))
	i := 0

	for _, c := range r.queue {
		queue[i] = c
		i++
	}

	return queue
}

func (r *replicator) Load(ctx context.Context, cids []cid.Cid) {
	cidsStrings := make([]string, len(cids))
	for i, c := range cids {
		cidsStrings[i] = c.String()
	}

	ctx, span := r.tracer.Start(ctx, "replicator-load", trace.WithAttributes(otkv.String("cids", strings.Join(cidsStrings, ","))))
	defer span.End()

	for _, h := range cids {
		_, inLog := r.store.OpLog().Get(h)
		r.lock.RLock()
		_, fetching := r.fetching[h.String()]
		_, queued := r.queue[h.String()]
		r.lock.RUnlock()

		if fetching || queued || inLog {
			continue
		}

		r.addToQueue(ctx, span, h)
	}

	r.processQueue(ctx)
}

type Options struct {
	Logger   *zap.Logger
	Tracer   trace.Tracer
	EventBus event.Bus
}

// NewReplicator Creates a new Replicator instance
func NewReplicator(ctx context.Context, store storeInterface, concurrency uint, opts *Options) (Replicator, error) {
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

	ctx, cancelFunc := context.WithCancel(ctx)

	if concurrency == 0 {
		concurrency = 128
	}

	r := replicator{
		eventBus:    opts.EventBus,
		cancelFunc:  cancelFunc,
		concurrency: int64(concurrency),
		store:       store,
		queue:       map[string]cid.Cid{},
		fetching:    map[string]cid.Cid{},
		logger:      opts.Logger,
		tracer:      opts.Tracer,
	}
	if err := r.generateEmitter(opts.EventBus); err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case <-time.After(time.Second * 3):
				r.lock.RLock()
				qLen := len(r.queue)
				r.lock.RUnlock()

				if r.tasksRunning() == 0 && qLen > 0 {
					r.logger.Debug(fmt.Sprintf("Had to flush the queue! %d items in the queue, %d %d tasks requested/finished", qLen, r.tasksRequested(), r.tasksFinished()))
					go r.processQueue(ctx)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return &r, nil
}

func (r *replicator) Close() error {

	return nil
}

func (r *replicator) tasksRunning() int64 {
	return atomic.LoadInt64(&r.statsTasksStarted) - atomic.LoadInt64(&r.statsTasksProcessed)
}

func (r *replicator) tasksRequested() int64 {
	return atomic.LoadInt64(&r.statsTasksRequested)
}

func (r *replicator) tasksFinished() int64 {
	return atomic.LoadInt64(&r.statsTasksProcessed)
}

func (r *replicator) processOne(ctx context.Context, h cid.Cid) ([]cid.Cid, error) {
	ctx, span := r.tracer.Start(ctx, "replicator-process-one")
	defer span.End()

	r.lock.Lock()
	defer r.lock.Unlock()

	_, isFetching := r.fetching[h.String()]
	_, hasEntry := r.store.OpLog().Get(h)

	if hasEntry || isFetching {
		return nil, nil
	}

	r.fetching[h.String()] = h

	if err := r.emitters.evtLoadAdded.Emit(NewEventLoadAdded(h)); err != nil {
		r.logger.Warn("unable to emit event load added", zap.Error(err))
	}

	atomic.AddInt64(&r.statsTasksStarted, 1)

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

	var logToAppend ipfslog.Log = l

	r.buffer = append(r.buffer, logToAppend)

	latest := l.Values().At(0)

	delete(r.queue, h.String())

	// Mark this task as processed
	//r.statsTasksProcessed++

	// Notify subscribers that we made progress
	if err := r.emitters.evtLoadProgress.Emit(NewEventLoadProgress("", h, latest, len(r.buffer))); err != nil {
		r.logger.Warn("unable to emit event load progress", zap.Error(err))
	}

	var nextValues []cid.Cid

	for _, e := range l.Values().Slice() {
		nextValues = append(nextValues, e.GetNext()...)
		nextValues = append(nextValues, e.GetRefs()...)
	}

	// Return all next pointers
	return nextValues, nil
}

func (r *replicator) processQueue(ctx context.Context) {
	if r.tasksRunning() >= r.concurrency {
		return
	}

	ctx, span := r.tracer.Start(ctx, "replicator-process-queue")
	defer span.End()

	capacity := r.concurrency - r.tasksRunning()
	slicedQueue := r.GetQueue()
	if int64(len(slicedQueue)) < capacity {
		capacity = int64(len(slicedQueue))
	}

	items := map[string]cid.Cid{}
	for _, h := range slicedQueue[:capacity] {
		items[h.String()] = h
	}

	var hashesList = make([][]cid.Cid, len(items))
	hashesListIdx := 0
	wg := sync.WaitGroup{}

	r.lock.Lock()
	for _, e := range items {
		delete(r.queue, e.String())
	}
	r.lock.Unlock()

	for _, e := range items {
		wg.Add(1)

		go func(hashesListIdx int, e cid.Cid) {
			defer wg.Done()

			hashes, err := r.processOne(ctx, e)
			if err != nil {
				r.logger.Error("unable to get data to process %v", zap.Error(err))
				return
			}

			hashesList[hashesListIdx] = hashes
		}(hashesListIdx, e)

		hashesListIdx++
	}

	wg.Wait()

	for _, hashes := range hashesList {
		r.lock.RLock()
		b := r.buffer
		bLen := len(b)
		r.lock.RUnlock()

		// Mark this task as processed
		atomic.AddInt64(&r.statsTasksProcessed, 1)

		if bLen > 0 && r.tasksRunning() == 0 {
			r.lock.Lock()
			r.buffer = []ipfslog.Log{}
			r.lock.Unlock()

			r.logger.Debug(fmt.Sprintf("load end logs, logs found :%d", bLen))

			if err := r.emitters.evtLoadEnd.Emit(NewEventLoadEnd(b)); err != nil {
				r.logger.Warn("unable to emit event load end", zap.Error(err))
			}
		}

		if len(hashes) > 0 {
			r.Load(ctx, hashes)
		}
	}
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

func (r *replicator) addToQueue(ctx context.Context, span trace.Span, h cid.Cid) {
	span.AddEvent("replicator-add-to-queue", trace.WithAttributes(otkv.String("cid", h.String())))

	r.lock.Lock()
	defer r.lock.Unlock()

	atomic.AddInt64(&r.statsTasksRequested, 1)
	r.queue[h.String()] = h
}

var _ Replicator = &replicator{}
