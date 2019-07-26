package kvstore

import (
	"berty.tech/go-ipfs-log/identityprovider"
	"context"
	orbitdb "github.com/berty/go-orbit-db"
	"github.com/berty/go-orbit-db/address"
	"github.com/berty/go-orbit-db/stores"
	"github.com/berty/go-orbit-db/stores/basestore"
	"github.com/berty/go-orbit-db/stores/operation"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
	"github.com/pkg/errors"
)

type orbitDBKeyValue struct {
	basestore.BaseStore
}

func (o *orbitDBKeyValue) All() map[string][]byte {
	return o.Index().(*kvIndex).index
}

func (o *orbitDBKeyValue) Put(ctx context.Context, key string, value []byte) (operation.Operation, error) {
	op := operation.NewOperation(&key, "PUT", value)

	e, err := o.AddOperation(ctx, op, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error while deleting value")
	}

	op, err = operation.ParseOperation(e)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse newly created entry")
	}

	return op, nil
}

func (o *orbitDBKeyValue) Delete(ctx context.Context, key string) (operation.Operation, error) {
	op := operation.NewOperation(&key, "DEL", nil)

	e, err := o.AddOperation(ctx, op, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error while deleting value")
	}

	op, err = operation.ParseOperation(e)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse newly created entry")
	}

	return op, nil
}

func (o *orbitDBKeyValue) Get(ctx context.Context, key string) ([]byte, error) {
	value, ok := o.Index().(*kvIndex).Get(key).([]byte)
	if value == nil {
		return nil, nil
	}

	if !ok {
		return nil, errors.New("unable to cast to bytes")
	}

	return value, nil
}

func (o *orbitDBKeyValue) Type() string {
	return "keyvalue"
}

func init() {
	stores.RegisterStore("keyvalue", NewOrbitDBKeyValue)
}

func NewOrbitDBKeyValue(ctx context.Context, ipfs coreapi.CoreAPI, identity *identityprovider.Identity, addr address.Address, options *orbitdb.NewStoreOptions) (i orbitdb.Store, e error) {
	store := &orbitDBKeyValue{}

	options.Index = NewEventIndex

	err := store.InitBaseStore(ctx, ipfs, identity, addr, options)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize base store")
	}

	return store, nil
}

var _ orbitdb.KeyValueStore = &orbitDBKeyValue{}
