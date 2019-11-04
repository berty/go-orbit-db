package basestore

import (
	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-orbit-db/iface"
)

type baseIndex struct {
	id    []byte
	index []ipfslog.Entry
}

func (b *baseIndex) Get(_ string) interface{} {
	return b.index
}

func (b *baseIndex) UpdateIndex(log ipfslog.Log, entries []ipfslog.Entry) error {
	b.index = log.Values().Slice()
	return nil
}

// NewBaseIndex Creates a new basic index
func NewBaseIndex(publicKey []byte) iface.StoreIndex {
	return &baseIndex{
		id: publicKey,
	}
}

var _ iface.IndexConstructor = NewBaseIndex
