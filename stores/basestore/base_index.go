package basestore

import (
	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	orbitdb "github.com/berty/go-orbit-db"
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

// NewBaseIndex Creates a new basic index
func NewBaseIndex(publicKey []byte) orbitdb.StoreIndex {
	return &baseIndex{
		id: publicKey,
	}
}

var _ orbitdb.IndexConstructor = NewBaseIndex
