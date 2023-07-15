package cache

import (
	"github.com/stateless-minds/go-orbit-db/address"
	datastore "github.com/ipfs/go-datastore"
	"go.uber.org/zap"
)

// Interface Cache interface
type Interface interface {
	// Load Loads a cache for a given database address and a root directory
	Load(directory string, dbAddress address.Address) (datastore.Datastore, error)

	// Close Closes a cache and all its associated data stores
	Close() error

	// Destroy Removes all the cached data for a database
	Destroy(directory string, dbAddress address.Address) error
}

type Options struct {
	Logger *zap.Logger
}
