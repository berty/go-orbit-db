// replicator the replication logic for an OrbitDB store
package replicator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-orbit-db/events"
	cid "github.com/ipfs/go-cid"
	"github.com/pkg/errors"
	otkv "go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/trace"
	"go.uber.org/zap"
)

var batchSize = 1

type replicator struct {
	events.EventEmitter

	cancelFunc          context.CancelFunc
	store               storeInterface
	fetching            map[string]cid.Cid
	statsTasksRequested uint
	statsTasksStarted   uint
	statsTasksProcessed uint
	buffer              []ipfslog.Log
	concurrency         uint
	queue               map[string]cid.Cid
	lock                sync.RWMutex
	logger              *zap.Logger
	tracer              trace.Tracer
}

func (r *replicator) GetBufferLen() int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return len(r.buffer)
}

func (r *replicator) Stop() {
	r.cancelFunc()
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
		inLog := r.store.OpLog().GetEntries().UnsafeGet(h.String()) != nil
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
	Logger *zap.Logger
	Tracer trace.Tracer
}

// NewReplicator Creates a new Replicator instance
func NewReplicator(ctx context.Context, store storeInterface, concurrency uint, opts *Options) Replicator {
	if opts == nil {
		opts = &Options{}
	}

	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}

	if opts.Tracer == nil {
		opts.Tracer = trace.NoopTracer{}
	}

	ctx, cancelFunc := context.WithCancel(ctx)

	if concurrency == 0 {
		concurrency = 128
	}

	r := replicator{
		cancelFunc:  cancelFunc,
		concurrency: concurrency,
		store:       store,
		queue:       map[string]cid.Cid{},
		fetching:    map[string]cid.Cid{},
		logger:      opts.Logger,
		tracer:      opts.Tracer,
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
					r.processQueue(ctx)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return &r
}

func (r *replicator) tasksRunning() uint {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.statsTasksStarted - r.statsTasksProcessed
}

func (r *replicator) tasksRequested() uint {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.statsTasksRequested
}

func (r *replicator) tasksFinished() uint {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.statsTasksProcessed
}

func (r *replicator) processOne(ctx context.Context, h cid.Cid) ([]cid.Cid, error) {
	ctx, span := r.tracer.Start(ctx, "replicator-process-one")
	defer span.End()

	r.lock.Lock()
	defer r.lock.Unlock()

	_, isFetching := r.fetching[h.String()]
	_, hasEntry := r.store.OpLog().Values().Get(h.String())

	if hasEntry || isFetching {
		return nil, nil
	}

	r.fetching[h.String()] = h

	r.Emit(ctx, NewEventLoadAdded(h))

	r.statsTasksStarted++

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
	r.statsTasksProcessed++

	// Notify subscribers that we made progress
	r.Emit(ctx, NewEventLoadProgress("", h, latest, len(r.buffer))) // TODO JS: this._id should be undefined

	var nextValues []cid.Cid

	for _, e := range l.Values().Slice() {
		nextValues = append(nextValues, e.GetNext()...)
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

	var hashesList [][]cid.Cid
	capacity := r.concurrency - r.tasksRunning()
	slicedQueue := r.GetQueue()
	if uint(len(slicedQueue)) < capacity {
		capacity = uint(len(slicedQueue))
	}

	items := map[string]cid.Cid{}
	for _, h := range slicedQueue[:capacity] {
		items[h.String()] = h
	}

	for _, e := range items {
		r.lock.Lock()
		delete(r.queue, e.String())
		r.lock.Unlock()

		hashes, err := r.processOne(ctx, e)
		if err != nil {
			r.logger.Error("unable to get data to process %v", zap.Error(err))
			return
		}

		hashesList = append(hashesList, hashes)
	}

	for _, hashes := range hashesList {
		r.lock.RLock()
		b := r.buffer
		bLen := len(b)
		r.lock.RUnlock()

		if (len(items) > 0 && bLen > 0) || (r.tasksRunning() == 0 && bLen > 0) {
			r.lock.Lock()
			r.buffer = []ipfslog.Log{}
			r.lock.Unlock()

			r.logger.Debug(fmt.Sprintf("load end logs, logs found :%d", bLen))

			r.Emit(ctx, NewEventLoadEnd(b))
		}

		if len(hashes) > 0 {
			r.Load(ctx, hashes)
		}
	}
}

func (r *replicator) addToQueue(ctx context.Context, span trace.Span, h cid.Cid) {
	span.AddEvent(ctx, "replicator-add-to-queue", otkv.String("cid", h.String()))

	r.lock.Lock()
	defer r.lock.Unlock()

	r.statsTasksRequested++
	r.queue[h.String()] = h
}

var _ Replicator = &replicator{}
