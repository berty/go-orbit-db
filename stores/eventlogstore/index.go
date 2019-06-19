package eventlogstore

import (
	"berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"github.com/berty/go-orbit-db/stores"
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

func NewEventIndex(_ []byte) stores.Index {
	return &eventIndex{}
}

var _ stores.IndexConstructor = NewEventIndex
var _ stores.Index = &eventIndex{}
