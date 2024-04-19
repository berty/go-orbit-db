package eventlogstore

import (
	"sync"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-orbit-db/iface"
)

type eventIndex struct {
	index ipfslog.Log
	lock  sync.RWMutex
}

func (i *eventIndex) Get(key string) interface{} { //nolint:all
	i.lock.RLock()
	defer i.lock.RUnlock()

	if i.index == nil {
		return nil
	}

	return i.index.Values().Slice()
}

func (i *eventIndex) UpdateIndex(log ipfslog.Log, _ []ipfslog.Entry) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	i.index = log

	return nil
}

// NewEventIndex Creates a new index for an EventLog Store
func NewEventIndex(_ []byte) iface.StoreIndex {
	return &eventIndex{}
}

var _ iface.IndexConstructor = NewEventIndex
var _ iface.StoreIndex = &eventIndex{}
