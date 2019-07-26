package replicator

import (
	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/identityprovider"
	"context"
	"github.com/berty/go-orbit-db/accesscontroller"
	"github.com/berty/go-orbit-db/events"
	"github.com/ipfs/go-cid"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
)

type StoreInterface interface {
	OpLog() *ipfslog.Log
	Ipfs() coreapi.CoreAPI
	Identity() *identityprovider.Identity
	AccessController() accesscontroller.Interface
}

type Replicator interface {
	events.EmitterInterface

	Stop()
	Load(ctx context.Context, cids []cid.Cid)
	GetQueue() []cid.Cid
	GetBufferLen() int
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
	DecreaseQueued(i int)
	SetBuffered(i int)
}
