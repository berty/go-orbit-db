package documentstore

import (
	"sync"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores/operation"
	"github.com/pkg/errors"
)

type documentIndex struct {
	iface.StoreIndex
	index   map[string]operation.Operation
	muIndex sync.RWMutex
}

func (i *documentIndex) Keys() []string {
	i.muIndex.RLock()
	defer i.muIndex.RUnlock()

	keys := make([]string, len(i.index))

	for key := range i.index {
		keys = append(keys, key)
	}

	return keys
}

func (i *documentIndex) Get(key string) interface{} {
    i.muIndex.RLock()
	defer i.muIndex.RUnlock()

	if i.index == nil {
		return nil
	}

	entry, ok := i.index[key]
	if !ok {
		return nil
	}

	return entry
}

func (i *documentIndex) UpdateIndex(oplog ipfslog.Log, _ []ipfslog.Entry) error {
	entries := oplog.Values().Slice()
	size := len(entries)

	handled := map[string]struct{}{}

	i.muIndex.Lock()
	defer i.muIndex.Unlock()

	for idx := range entries {
		item, err := operation.ParseOperation(entries[size-idx-1])
		if err != nil {
			return errors.Wrap(err, "unable to parse log documentstore operation")
		}

		key := item.GetKey()
		if key == nil {
			// ignoring entries with nil keys
			continue
		}

		if _, ok := handled[*item.GetKey()]; !ok {
			handled[*item.GetKey()] = struct{}{}

			if item.GetOperation() == "PUT" {
				i.index[*item.GetKey()] = item
			} else if item.GetOperation() == "DEL" {
				delete(i.index, *item.GetKey())
			}
		}
	}

	return nil
}

// NewDocumentIndex Creates a new index for a Document Store
func NewDocumentIndex(_ []byte) iface.StoreIndex {
	return &documentIndex{
		index: make(map[string]operation.Operation),
	}
}

var _ iface.IndexConstructor = NewDocumentIndex
var _ iface.StoreIndex = &documentIndex{}
