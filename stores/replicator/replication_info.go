package replicator

import "sync"

type replicationInfo struct {
	Progress int
	Max      int
	Buffered int
	Queued   int
	lock     sync.RWMutex
}

func (r *replicationInfo) SetBuffered(i int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.Buffered = i
}

func (r *replicationInfo) SetProgress(i int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.Progress = i
}

func (r *replicationInfo) SetMax(i int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.Max = i
}

func (r *replicationInfo) IncQueued() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.Queued++
}

func (r *replicationInfo) GetProgress() int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.Progress
}

func (r *replicationInfo) GetMax() int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.Max
}

func (r *replicationInfo) GetBuffered() int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.Buffered
}

func (r *replicationInfo) GetQueued() int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.Queued
}

func (r *replicationInfo) Reset() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.Progress = 0
	r.Max = 0
	r.Buffered = 0
	r.Queued = 0
}

func (r *replicationInfo) DecreaseQueued(amount int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.Queued -= amount
}

// NewReplicationInfo Creates a new ReplicationInfo instance
func NewReplicationInfo() ReplicationInfo {
	return &replicationInfo{}
}

var _ ReplicationInfo = &replicationInfo{}
