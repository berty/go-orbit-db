package eventlogstore

import (
	"berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"github.com/berty/go-orbit-db/iface"
)

type eventIndex struct {
	index *ipfslog.Log
}

func (i *eventIndex) Get(key string) interface{} {
	if i.index == nil {
		return nil
	}

	return i.index.Values().Slice()
}

func (i *eventIndex) UpdateIndex(log *ipfslog.Log, _ []*entry.Entry) error {
	i.index = log

	return nil
}

// NewEventIndex Creates a new index for an EventLog Store
func NewEventIndex(_ []byte) iface.StoreIndex {
	return &eventIndex{}
}

var _ iface.IndexConstructor = NewEventIndex
var _ iface.StoreIndex = &eventIndex{}
