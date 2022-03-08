// replicator the replication logic for an OrbitDB store
package replicator

import (
	"context"
	"fmt"
	"strings"
	"sync"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/iface"
	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/pkg/errors"
	otkv "go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

var batchSize = 1

type queuedState int

const (
	stateAdded queuedState = iota
	stateFetching
	stateFetched
)

type replicator struct {
	eventBus event.Bus
	emitters struct {
		evtLoadEnd      event.Emitter
		evtLoadAdded    event.Emitter
		evtLoadProgress event.Emitter
	}

	taskInProgress int64

	store       storeInterface
	concurrency int64

	tasks map[cid.Cid]queuedState

	sem       *semaphore.Weighted
	queue     *processQueue
	muProcess sync.RWMutex

	buffer   []ipfslog.Log
	muBuffer sync.Mutex
	logger   *zap.Logger
	tracer   trace.Tracer
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

	r := replicator{
		eventBus:    opts.EventBus,
		concurrency: int64(concurrency),
		store:       store,
		tasks:       make(map[cid.Cid]queuedState),
		queue:       &processQueue{},
		logger:      opts.Logger,
		tracer:      opts.Tracer,
		sem:         semaphore.NewWeighted(int64(concurrency)),
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

	fetching := make([]cid.Cid, r.queue.Len())
	i := 0
	for c := range r.tasks {
		fetching[i] = c
		i++
	}

	return fetching
}

func (r *replicator) Load(ctx context.Context, entries []ipfslog.Entry) {
	cidsStrings := make([]string, len(entries))
	for i, e := range entries {
		cidsStrings[i] = e.GetHash().String()
	}

	ctx, span := r.tracer.Start(ctx, "replicator-load", trace.WithAttributes(otkv.String("cids", strings.Join(cidsStrings, ","))))
	defer span.End()

	// process and wait the whole queue to complete
	wg := sync.WaitGroup{}

	r.muProcess.Lock()
	for i, entry := range entries {
		if exist := r.AddEntryToQueue(entry); exist {
			continue
		}

		// signal that we add an entry to the queue
		if err := r.emitters.evtLoadAdded.Emit(NewEventLoadAdded(entry.GetHash(), entry)); err != nil {
			r.logger.Warn("unable to emit event load added", zap.Error(err))
		}

		wg.Add(1)
		// add one process
		go func(i int) {
			if err := r.processOne(ctx, &wg); err != nil {
				r.logger.Warn("unable to process entry", zap.Error(err))
			}

			wg.Done()
		}(i)
	}
	r.muProcess.Unlock()

	wg.Wait()
}

// processOne wait for a process slot then process one element of the queue
func (r *replicator) processOne(ctx context.Context, wg *sync.WaitGroup) error {
	// wait for a process slot
	e, err := r.waitForProcessSlot(ctx)
	if err != nil {
		return err
	}

	if err := r.processItems(ctx, wg, e); err != nil {
		r.logger.Warn("process item ended", zap.Error(err))
	}

	// mark this process has done
	r.processEntryDone(e)
	return nil
}

// processItems process an entry then add to the queue every next entry
func (r *replicator) processItems(ctx context.Context, wg *sync.WaitGroup, items ...processItem) error {
	// mark this entry has done
	for _, item := range items {
		next, err := r.processHash(ctx, item)
		if err != nil {
			return err
		}

		r.muProcess.Lock()
		for _, hash := range next {
			if exist := r.AddHashToQueue(hash); exist {
				continue
			}

			wg.Add(1)

			// add process
			go func() {
				if err := r.processOne(ctx, wg); err != nil {
					r.logger.Warn("unable to process entry", zap.Error(err))
				}

				wg.Done()
			}()
		}
		r.muProcess.Unlock()
	}

	return nil
}

func (r *replicator) processHash(ctx context.Context, item processItem) ([]cid.Cid, error) {
	hash := item.GetHash()

	// @FIXME(gfanton): chan progress should be created and close on ipfs-log
	cprogress := make(chan iface.IPFSLogEntry)
	defer close(cprogress)
	go func() {
		var entry iface.IPFSLogEntry
		for {

			select {
			case <-ctx.Done():
				return
			case entry = <-cprogress:
			}

			if entry == nil {
				return
			}

			if err := r.emitters.evtLoadProgress.Emit(NewEventLoadProgress(entry)); err != nil {
				r.logger.Warn("unable to emit event load progress", zap.Error(err))
			}
		}
	}()

	l, err := ipfslog.NewFromEntryHash(ctx, r.store.IPFS(), r.store.Identity(), hash, &ipfslog.LogOptions{
		ID:               r.store.OpLog().GetID(),
		AccessController: r.store.AccessController(),
		SortFn:           r.store.SortFn(),
		IO:               r.store.IO(),
	}, &ipfslog.FetchOptions{
		Length:        &batchSize,
		ProgressChan:  cprogress,
		ShouldExclude: r.shouldExclude,
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to fetch log")
	}

	r.muBuffer.Lock()
	r.buffer = append(r.buffer, l)
	r.muBuffer.Unlock()

	var nextValues []cid.Cid
	for _, e := range l.Values().Slice() {
		nextValues = append(nextValues, e.GetNext()...)
		nextValues = append(nextValues, e.GetRefs()...)
	}

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

func (r *replicator) waitForProcessSlot(ctx context.Context) (e processItem, err error) {
	if err := r.sem.Acquire(ctx, 1); err != nil {
		return nil, fmt.Errorf("failed to acquire process slot: %w", err)
	}
	r.muProcess.Lock()

	r.taskInProgress++

	e = r.queue.Next()
	r.tasks[e.GetHash()] = stateFetching

	r.muProcess.Unlock()
	return
}

func (r *replicator) processEntryDone(item processItem) {
	r.muProcess.Lock()

	r.taskInProgress--

	// remove hash from queued list
	r.tasks[item.GetHash()] = stateFetched

	// if there no more task to proceed, trigger idle method
	if r.isIdle() {
		r.idle()
	}

	// signal that a process slot is available
	r.sem.Release(1)

	r.muProcess.Unlock()
}

func (r *replicator) shouldExclude(hash cid.Cid) (exist bool) {
	r.muProcess.RLock()
	defer r.muProcess.RUnlock()

	if _, inLog := r.store.OpLog().Get(hash); inLog {
		return true
	}

	if task, queued := r.tasks[hash]; queued && task == stateFetched {
		return true
	}

	return false
}

// AddHashToQueue is not thread safe
func (r *replicator) AddHashToQueue(hash cid.Cid) (exist bool) {
	_, inLog := r.store.OpLog().Get(hash)
	_, queued := r.tasks[hash]
	if exist = queued || inLog; exist {
		return
	}

	item := newProcessHash(hash)
	r.queue.Add(item)
	r.tasks[hash] = stateAdded
	return
}

// AddEntryToQueue is not thread safe
func (r *replicator) AddEntryToQueue(entry iface.IPFSLogEntry) (exist bool) {
	hash := entry.GetHash()
	_, inLog := r.store.OpLog().Get(hash)
	_, queued := r.tasks[hash]
	if exist = queued || inLog; exist {
		return
	}

	item := newProcessEntry(entry)
	r.queue.Add(item)
	r.tasks[hash] = stateAdded
	return
}

// isIdle is not thread safe
func (r *replicator) isIdle() bool {
	if r.taskInProgress > 0 && r.queue.Len() > 0 {
		return false
	}

	for _, kind := range r.tasks {
		if kind == stateAdded || kind == stateFetching {
			return false
		}
	}

	return true
}

// idle is not thread safe
func (r *replicator) idle() {
	r.muBuffer.Lock()

	if len(r.buffer) > 0 {
		if err := r.emitters.evtLoadEnd.Emit(NewEventLoadEnd(r.buffer)); err != nil {
			r.logger.Warn("unable to emit event load end", zap.Error(err))
		}
		r.buffer = []ipfslog.Log{}

		// @FIXME: use better garbage collector here
		// for h, s := range r.tasks {
		// 	if s == stateFetched {
		// 		delete(r.tasks, h)
		// 	}
		// }
	}

	r.muBuffer.Unlock()
}

var _ Replicator = &replicator{}
