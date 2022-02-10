package replicator

import "sync"

type replicationInfo struct {
	progress int
	max      int
	lock     sync.RWMutex
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

func (r *replicationInfo) Reset() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.progress = 0
	r.max = 0
}

// NewReplicationInfo Creates a new ReplicationInfo instance
func NewReplicationInfo() ReplicationInfo {
	return &replicationInfo{}
}

var _ ReplicationInfo = &replicationInfo{}
