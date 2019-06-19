package basestore

import (
	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"github.com/berty/go-orbit-db/stores"
)

type baseIndex struct {
	id    []byte
	index []*entry.Entry
}

func (b *baseIndex) Get(_ string) interface{} {
	return b.index
}

func (b *baseIndex) UpdateIndex(log *ipfslog.Log, entries []*entry.Entry) error {
	b.index = log.Values().Slice()
	return nil
}

func NewBaseIndex(publicKey []byte) stores.Index {
	return &baseIndex{
		id: publicKey,
	}
}

var _ stores.IndexConstructor = NewBaseIndex
