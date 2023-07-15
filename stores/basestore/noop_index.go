package basestore

import (
	ipfslog "github.com/stateless-minds/go-ipfs-log"

	"github.com/stateless-minds/go-orbit-db/iface"
)

type noopIndex struct{}

func (b *noopIndex) Get(_ string) interface{}                           { return nil }
func (b *noopIndex) UpdateIndex(_ ipfslog.Log, _ []ipfslog.Entry) error { return nil }

// NewBaseIndex Creates a new basic index
func NewNoopIndex(_ []byte) iface.StoreIndex {
	return &noopIndex{}
}

var _ iface.IndexConstructor = NewNoopIndex
