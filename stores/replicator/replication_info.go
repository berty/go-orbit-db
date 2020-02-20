package replicator

import "sync"

type replicationInfo struct {
	progress int
	max      int
	buffered int
	queued   int
	lock     sync.RWMutex
}

func (r *replicationInfo) SetBuffered(i int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.buffered = i
}

func (r *replicationInfo) SetProgress(i int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.progress = i
}

func (r *replicationInfo) SetMax(i int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.max = i
}

func (r *replicationInfo) IncQueued() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.queued++
}

func (r *replicationInfo) GetProgress() int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.progress
}

func (r *replicationInfo) GetMax() int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.max
}

func (r *replicationInfo) GetBuffered() int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.buffered
}

func (r *replicationInfo) GetQueued() int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.queued
}

func (r *replicationInfo) Reset() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.progress = 0
	r.max = 0
	r.buffered = 0
	r.queued = 0
}

func (r *replicationInfo) DecreaseQueued(amount int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.queued -= amount
}

// NewReplicationInfo Creates a new ReplicationInfo instance
func NewReplicationInfo() ReplicationInfo {
	return &replicationInfo{}
}

var _ ReplicationInfo = &replicationInfo{}
