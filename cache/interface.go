package cache

import (
	"github.com/berty/go-orbit-db/address"
	"github.com/ipfs/go-datastore"
)

type Interface interface {
	Load(directory string, dbAddress address.Address) (datastore.Datastore, error)
	Close() error
}