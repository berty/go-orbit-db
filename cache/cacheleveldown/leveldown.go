package cacheleveldown

import (
	"fmt"
	"github.com/berty/go-orbit-db/address"
	"github.com/berty/go-orbit-db/cache"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/pkg/errors"
	"path"
)

var singleton cache.Interface = nil

type levelDownCache struct {
	caches map[string]datastore.Datastore
}

func (l *levelDownCache) Load(directory string, dbAddress address.Address) (datastore.Datastore, error) {
	dbPath := path.Join(dbAddress.GetRoot().String(), dbAddress.GetPath())
	cachePath := path.Join(directory, dbPath)
	var ds datastore.Datastore

	if c, ok := l.caches[cachePath]; ok {
		return c, nil
	}

	logger().Debug(fmt.Sprintf("opening cache db from path %s", cachePath))

	ds, err := leveldb.NewDatastore(cachePath, nil)

	ds = datastore.NewLogDatastore(ds, "cache-ds")

	if err != nil {
		return nil, errors.Wrap(err, "unable to init leveldb datastore")
	}

	l.caches[cachePath] = ds

	return ds, nil
}

func (l *levelDownCache) Close() error {
	for k, c := range l.caches {
		_ = c.Close() // TODO: handle error
		delete(l.caches, k)
	}

	return nil
}

func New() cache.Interface {
	if singleton != nil {
		return singleton
	}

	singleton = &levelDownCache{
		caches: map[string]datastore.Datastore{},
	}

	return singleton
}

var _ cache.Interface = &levelDownCache{}