package documentstore

import (
	"fmt"
	"sync"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores/operation"
)

type documentIndex struct {
	iface.StoreIndex
	index   map[string][]byte
	muIndex sync.RWMutex
	opts    *iface.CreateDocumentDBOptions
}

func (i *documentIndex) Keys() []string {
	i.muIndex.RLock()
	defer i.muIndex.RUnlock()

	keys := make([]string, len(i.index))

	idx := 0
	for key := range i.index {
		keys[idx] = key
		idx++
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
			return fmt.Errorf("unable to parse log documentstore operation: %w", err)
		}

		if item.GetOperation() == "PUTALL" {
			for _, opDoc := range item.GetDocs() {
				if _, ok := handled[opDoc.GetKey()]; ok {
					continue
				}

				handled[*item.GetKey()] = struct{}{}
				i.index[opDoc.GetKey()] = opDoc.GetValue()
			}

			continue
		}

		key := item.GetKey()
		if key == nil || *key == "" {
			// ignoring entries with nil or empty keys
			continue
		}

		if _, ok := handled[*item.GetKey()]; ok {
			continue
		}

		handled[*item.GetKey()] = struct{}{}
		switch item.GetOperation() {
		case "PUT":
			i.index[*item.GetKey()] = item.GetValue()

		case "DEL":
			delete(i.index, *item.GetKey())
		}
	}

	return nil
}

// newDocumentIndex Creates a new index for a Document Store
func newDocumentIndex(opts *iface.CreateDocumentDBOptions) iface.StoreIndex {
	return &documentIndex{
		index: map[string][]byte{},
		opts:  opts,
	}
}

var _ iface.StoreIndex = &documentIndex{}
