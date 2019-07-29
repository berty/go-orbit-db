package replicator

type replicationInfo struct {
	Progress int
	Max      int
	Buffered int
	Queued   int
}

func (r *replicationInfo) SetBuffered(i int) {
	r.Buffered = i
}

func (r *replicationInfo) SetProgress(i int) {
	r.Progress = i
}

func (r *replicationInfo) SetMax(i int) {
	r.Max = i
}

func (r *replicationInfo) IncQueued() {
	r.Queued++
}

func (r *replicationInfo) GetProgress() int {
	return r.Progress
}

func (r *replicationInfo) GetMax() int {
	return r.Max
}

func (r *replicationInfo) GetBuffered() int {
	return r.Buffered
}

func (r *replicationInfo) GetQueued() int {
	return r.Queued
}

func (r *replicationInfo) Reset() {
	r.Progress = 0
	r.Max = 0
	r.Buffered = 0
	r.Queued = 0
}

func (r *replicationInfo) DecreaseQueued(amount int) {
	r.Queued -= amount
}

// NewReplicationInfo Creates a new ReplicationInfo instance
func NewReplicationInfo() ReplicationInfo {
	return &replicationInfo{}
}

var _ ReplicationInfo = &replicationInfo{}
