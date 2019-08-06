package replicator

import (
	ipfslog "berty.tech/go-ipfs-log"
	"context"
	"fmt"
	"github.com/berty/go-orbit-db/events"
	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
	"time"
)

var batchSize = 1

type replicator struct {
	events.EventEmitter

	cancelFunc          context.CancelFunc
	store               StoreInterface
	fetching            map[string]cid.Cid
	statsTasksRequested uint
	statsTasksStarted   uint
	statsTasksProcessed uint
	buffer              []*ipfslog.Log
	concurrency         uint
	queue               map[string]cid.Cid
}

func (r *replicator) GetBufferLen() int {
	return len(r.buffer)
}

func (r *replicator) Stop() {
	r.cancelFunc()
}

func (r *replicator) GetQueue() []cid.Cid {
	var queue []cid.Cid

	for _, c := range r.queue {
		queue = append(queue, c)
	}

	return queue
}

func (r *replicator) Load(ctx context.Context, cids []cid.Cid) {
	for _, h := range cids {
		inLog := r.store.OpLog().Entries.UnsafeGet(h.String()) != nil
		_, fetching := r.fetching[h.String()]
		_, queued := r.queue[h.String()]

		if fetching || queued || inLog {
			continue
		}

		r.addToQueue(h)
	}

	r.processQueue(ctx)
}

// NewReplicator Creates a new Replicator instance
func NewReplicator(ctx context.Context, store StoreInterface, concurrency uint) Replicator {
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
	}

	go func() {
		for {
			select {
			case <-time.After(time.Second * 3):
				if r.tasksRunning() == 0 && len(r.queue) > 0 {
					logger().Debug(fmt.Sprintf("Had to flush the queue! %d items in the queue, %d %d tasks requested/finished", len(r.queue), r.tasksRequested(), r.tasksFinished()))
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
	return r.statsTasksStarted - r.statsTasksProcessed
}

func (r *replicator) tasksRequested() uint {
	return r.statsTasksRequested
}

func (r *replicator) tasksFinished() uint {
	return r.statsTasksProcessed
}

func (r *replicator) queueSlice() []cid.Cid {
	var slice []cid.Cid

	for _, v := range r.queue {
		slice = append(slice, v)
	}

	return slice
}

func (r *replicator) processOne(ctx context.Context, h cid.Cid) ([]cid.Cid, error) {
	_, isFetching := r.fetching[h.String()]
	_, hasEntry := r.store.OpLog().Values().Get(h.String())

	if hasEntry || isFetching {
		return nil, nil
	}

	r.fetching[h.String()] = h

	r.Emit(NewEventLoadAdded(h))

	r.statsTasksStarted++

	l, err := ipfslog.NewFromEntryHash(ctx, r.store.Ipfs(), r.store.Identity(), h, &ipfslog.LogOptions{
		ID:               r.store.OpLog().ID,
		AccessController: r.store.AccessController(),
	}, &ipfslog.FetchOptions{
		Length: &batchSize,
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to fetch log")
	}

	r.buffer = append(r.buffer, l)

	latest := l.Values().At(0)

	delete(r.queue, h.String())

	// Mark this task as processed
	r.statsTasksProcessed++

	// Notify subscribers that we made progress
	r.Emit(NewEventLoadProgress("", h, latest, nil, len(r.buffer))) // TODO JS: this._id should be undefined

	var nextValues []cid.Cid

	for _, e := range l.Values().Slice() {
		for _, n := range e.Next {
			nextValues = append(nextValues, n)
		}
	}

	// Return all next pointers
	return nextValues, nil
}

func (r *replicator) processQueue(ctx context.Context) {
	if r.tasksRunning() >= r.concurrency {
		return
	}

	var hashesList [][]cid.Cid
	capacity := r.concurrency - r.tasksRunning()
	slicedQueue := r.queueSlice()
	if uint(len(slicedQueue)) < capacity {
		capacity = uint(len(slicedQueue))
	}

	items := map[string]cid.Cid{}
	for _, h := range slicedQueue[:capacity] {
		items[h.String()] = h
	}

	for _, e := range items {
		delete(r.queue, e.String())
		hashes, err := r.processOne(ctx, e)
		if err != nil {
			log.Errorf("unable to get data to process %v", err)
			return
		}

		hashesList = append(hashesList, hashes)
	}

	for _, hashes := range hashesList {
		if (len(items) > 0 && len(r.buffer) > 0) ||
			(r.tasksRunning() == 0 && len(r.buffer) > 0) {

			logs := r.buffer
			r.buffer = []*ipfslog.Log{}

			logger().Debug(fmt.Sprintf("load end logs, logs found :%d", len(logs)))

			r.Emit(NewEventLoadEnd(logs))
		}

		if len(hashes) > 0 {
			r.Load(ctx, hashes)
		}
	}
}

func (r *replicator) addToQueue(h cid.Cid) {
	r.statsTasksRequested++
	r.queue[h.String()] = h
}

var _ Replicator = &replicator{}
