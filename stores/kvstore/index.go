package kvstore

import (
	"berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	orbitdb "github.com/berty/go-orbit-db"
	"github.com/berty/go-orbit-db/stores/operation"
	"github.com/pkg/errors"
)

type kvIndex struct {
	index map[string][]byte
}

func (i *kvIndex) Get(key string) interface{} {
	return i.index[key]
}

func (i *kvIndex) UpdateIndex(oplog *ipfslog.Log, _ []*entry.Entry) error {
	entries := oplog.Values().Slice()
	size := len(entries)

	handled := map[string]struct{}{}

	for idx := range entries {
		item, err := operation.ParseOperation(entries[size-idx-1])
		if err != nil {
			return errors.Wrap(err, "unable to parse log kv operation")
		}

		key := item.GetKey()
		if key == nil {
			// ignoring entries with nil keys
			continue
		}

		if _, ok := handled[*item.GetKey()]; !ok {
			handled[*item.GetKey()] = struct{}{}

			if item.GetOperation() == "PUT" {
				i.index[*item.GetKey()] = item.GetValue()
			} else if item.GetOperation() == "DEL" {
				delete(i.index, *item.GetKey())
			}
		}
	}

	return nil
}

func NewEventIndex(_ []byte) orbitdb.StoreIndex {
	return &kvIndex{
		index: map[string][]byte{},
	}
}

var _ orbitdb.IndexConstructor = NewEventIndex
var _ orbitdb.StoreIndex = &kvIndex{}
