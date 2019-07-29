// cacheleveldown is a package returning level db data stores for OrbitDB
package cacheleveldown

import (
	"fmt"
	"github.com/berty/go-orbit-db/address"
	"github.com/berty/go-orbit-db/cache"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/pkg/errors"
	"os"
	"path"
)

var singleton cache.Interface

type levelDownCache struct {
	caches map[string]*wrappedCache
}

type wrappedCache struct {
	wrappedCache datastore.Datastore
	manager      *levelDownCache
	id           string
}

func (w *wrappedCache) Get(key datastore.Key) (value []byte, err error) {
	return w.wrappedCache.Get(key)
}

func (w *wrappedCache) Has(key datastore.Key) (exists bool, err error) {
	return w.wrappedCache.Has(key)
}

func (w *wrappedCache) GetSize(key datastore.Key) (size int, err error) {
	return w.wrappedCache.GetSize(key)
}

func (w *wrappedCache) Query(q query.Query) (query.Results, error) {
	return w.wrappedCache.Query(q)
}

func (w *wrappedCache) Put(key datastore.Key, value []byte) error {
	return w.wrappedCache.Put(key, value)
}

func (w *wrappedCache) Delete(key datastore.Key) error {
	return w.wrappedCache.Delete(key)
}

func (w *wrappedCache) Close() error {
	err := w.wrappedCache.Close()
	delete(w.manager.caches, w.id)
	return err
}

func (l *levelDownCache) Load(directory string, dbAddress address.Address) (datastore.Datastore, error) {
	cachePath := datastoreKey(directory, dbAddress)
	var ds datastore.Datastore

	if c, ok := l.caches[cachePath]; ok {
		return c, nil
	}

	logger().Debug(fmt.Sprintf("opening cache db from path %s", cachePath))

	ds, err := leveldb.NewDatastore(cachePath, nil)

	//ds = datastore.NewLogDatastore(ds, "cache-ds")

	if err != nil {
		return nil, errors.Wrap(err, "unable to init leveldb datastore")
	}

	l.caches[cachePath] = &wrappedCache{wrappedCache: ds, id: cachePath, manager: l}

	return ds, nil
}

func (l *levelDownCache) Close() error {
	for k, c := range l.caches {
		_ = c.Close()
		delete(l.caches, k)
	}

	return nil
}

func datastoreKey(directory string, dbAddress address.Address) string {
	dbPath := path.Join(dbAddress.GetRoot().String(), dbAddress.GetPath())
	cachePath := path.Join(directory, dbPath)

	return cachePath
}

func (l *levelDownCache) Destroy(directory string, dbAddress address.Address) error {
	err := os.RemoveAll(datastoreKey(directory, dbAddress))
	if err != nil {
		return errors.Wrap(err, "unable to delete datastore")
	}

	return nil
}

// New Creates a new leveldb data store
func New() cache.Interface {
	if singleton != nil {
		return singleton
	}

	singleton = &levelDownCache{
		caches: map[string]*wrappedCache{},
	}

	return singleton
}

var _ cache.Interface = &levelDownCache{}
var _ datastore.Datastore = &wrappedCache{}
