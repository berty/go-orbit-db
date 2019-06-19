package replicator

import (
	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/identityprovider"
	"context"
	"github.com/berty/go-orbit-db/accesscontroller"
	"github.com/berty/go-orbit-db/ipfs"
	"github.com/ipfs/go-cid"
)

type StoreInterface interface {
	OpLog() *ipfslog.Log
	Ipfs() ipfs.Services
	Identity() *identityprovider.Identity
	AccessController() accesscontroller.SimpleInterface
}

type Replicator interface {
	Stop()
	Load(ctx context.Context, cids []cid.Cid)
	Subscribe(c chan Event)
	Unsubscribe(c chan Event)
	GetQueue() []cid.Cid
}

type ReplicationInfo interface {
	GetProgress() int
	GetMax() int
	GetBuffered() int
	GetQueued() int
	IncQueued()
	Reset()
	SetProgress(i int)
	SetMax(i int)
}

type replicationInfo struct {
	Progress int
	Max      int
	Buffered int
	Queued   int
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

func NewReplicationInfo() ReplicationInfo {
	return &replicationInfo{}
}

var _ ReplicationInfo = &replicationInfo{}
