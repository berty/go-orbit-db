package documentstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-orbit-db/address"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores/basestore"
	"berty.tech/go-orbit-db/stores/operation"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
	"github.com/pkg/errors"
)

type orbitDBDocumentStore struct {
	basestore.BaseStore

	indexBy string
}

func (o *orbitDBDocumentStore) Get(ctx context.Context, key string, caseSensitive bool) ([]map[string]interface{}, error) {
	numTerms := len(strings.Split(key, " "))
	if numTerms > 1 {
		key = strings.ReplaceAll(key, ".", " ")
		key = strings.ToLower(key)
	} else {
		key = strings.ToLower(key)
	}

	docIndex, ok := o.Index().(*documentIndex)
	if !ok {
		return nil, errors.New("unable to cast index to documentIndex")
	}

	documents := make([]map[string]interface{}, 0)

	for _, indexKey := range docIndex.Keys() {
		if caseSensitive && strings.Contains(indexKey, key) {
			op, ok := o.Index().Get(indexKey).(operation.Operation)
			if !ok {
				return nil, errors.New("unable to cast document to operation")
			}
			var valueJSON map[string]interface{}
			err := json.Unmarshal(op.GetValue(), &valueJSON)
			if err != nil {
				return nil, errors.Wrap(err, "unable to unmarshal index content")
			}
			documents = append(documents, valueJSON)
		}
		if !caseSensitive {
			if numTerms > 1 {
				indexKey = strings.ReplaceAll(indexKey, ".", " ")
			}
			lower := strings.ToLower(indexKey)

			if strings.Contains(lower, key) {
				op, ok := o.Index().Get(indexKey).(operation.Operation)
				if !ok {
					return nil, errors.New("unable to cast document to operation")
				}
				var valueJSON map[string]interface{}
				err := json.Unmarshal(op.GetValue(), &valueJSON)
				if err != nil {
					return nil, errors.Wrap(err, "unable to unmarshal index content")
				}
				documents = append(documents, valueJSON)
			}
		}
	}

	return documents, nil
}

func (o *orbitDBDocumentStore) Put(ctx context.Context, document map[string]interface{}) (operation.Operation, error) {

	index, ok := document[o.indexBy]
	if !ok {
		return nil, fmt.Errorf("index '%s' not present in value", index)
	}

	indexStr, ok := index.(string)
	if !ok {
		return nil, errors.New("unable to cast index to string")
	}

	documentJSON, err := json.Marshal(document)
	if err != nil {
		return nil, errors.Wrapf(err, "failed marshaling value to json")
	}

	op := operation.NewOperation(&indexStr, "PUT", documentJSON)

	e, err := o.AddOperation(ctx, op, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error while adding operation")
	}

	op, err = operation.ParseOperation(e)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse newly created entry")
	}

	return op, nil
}

func (o *orbitDBDocumentStore) Delete(ctx context.Context, key string) (operation.Operation, error) {
	if e := o.Index().Get(key); e == nil {
		return nil, fmt.Errorf("no entry with key '%s' in database", key)
	}

	op := operation.NewOperation(&key, "DEL", nil)

	e, err := o.AddOperation(ctx, op, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error while adding operation")
	}

	op, err = operation.ParseOperation(e)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse newly created entry")
	}

	return op, nil
}

func (o *orbitDBDocumentStore) Type() string {
	return "docstore"
}

// NewOrbitDBDocumentStore Instantiates a new DocumentStore
func NewOrbitDBDocumentStore(ctx context.Context, ipfs coreapi.CoreAPI, identity *identityprovider.Identity, addr address.Address, options *iface.NewStoreOptions) (iface.Store, error) {
	store := &orbitDBDocumentStore{}
	options.Index = NewDocumentIndex

	// TODO: How can we pass this via options?
	store.indexBy = "_id"

	err := store.InitBaseStore(ctx, ipfs, identity, addr, options)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize base store")
	}

	return store, nil
}

var _ iface.DocumentStore = &orbitDBDocumentStore{}
