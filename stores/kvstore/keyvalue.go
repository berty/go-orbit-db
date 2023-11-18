package kvstore

import (
	"context"
	"fmt"

	"berty.tech/go-ipfs-log/identityprovider"
	coreapi "github.com/ipfs/boxo/coreiface"

	"berty.tech/go-orbit-db/address"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores/basestore"
	"berty.tech/go-orbit-db/stores/operation"
)

type orbitDBKeyValue struct {
	basestore.BaseStore
}

func (o *orbitDBKeyValue) All() map[string][]byte {
	idx, ok := o.Index().(*kvIndex)
	if !ok {
		return map[string][]byte{}
	}

	idx.muIndex.RLock()
	defer idx.muIndex.RUnlock()

	copiedIndex := map[string][]byte{}

	for k, v := range idx.index {
		copiedIndex[k] = v
	}

	return copiedIndex
}

func (o *orbitDBKeyValue) Put(ctx context.Context, key string, value []byte) (operation.Operation, error) {
	op := operation.NewOperation(&key, "PUT", value)

	e, err := o.AddOperation(ctx, op, nil)
	if err != nil {
		return nil, fmt.Errorf("error while adding value: %w", err)
	}

	op, err = operation.ParseOperation(e)
	if err != nil {
		return nil, fmt.Errorf("unable to parse newly created entry: %w", err)
	}

	return op, nil
}

func (o *orbitDBKeyValue) Delete(ctx context.Context, key string) (operation.Operation, error) {
	op := operation.NewOperation(&key, "DEL", nil)

	e, err := o.AddOperation(ctx, op, nil)
	if err != nil {
		return nil, fmt.Errorf("error while deleting value: %w", err)
	}

	op, err = operation.ParseOperation(e)
	if err != nil {
		return nil, fmt.Errorf("unable to parse newly created entry: %w", err)
	}

	return op, nil
}

func (o *orbitDBKeyValue) Get(ctx context.Context, key string) ([]byte, error) {
	value, ok := o.Index().(*kvIndex).Get(key).([]byte)
	if value == nil {
		return nil, nil
	}

	if !ok {
		return nil, fmt.Errorf("unable to cast to bytes")
	}

	return value, nil
}

func (o *orbitDBKeyValue) Type() string {
	return "keyvalue"
}

// NewOrbitDBKeyValue Instantiates a new KeyValueStore
func NewOrbitDBKeyValue(ipfs coreapi.CoreAPI, identity *identityprovider.Identity, addr address.Address, options *iface.NewStoreOptions) (i iface.Store, e error) {
	store := &orbitDBKeyValue{}

	options.Index = NewKVIndex

	err := store.InitBaseStore(ipfs, identity, addr, options)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize base store: %w", err)
	}

	return store, nil
}

var _ iface.KeyValueStore = &orbitDBKeyValue{}
