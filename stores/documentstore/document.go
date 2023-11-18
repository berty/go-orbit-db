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

	coreapi "github.com/ipfs/boxo/coreiface"
)

type orbitDBDocumentStore struct {
	basestore.BaseStore
	docOpts *iface.CreateDocumentDBOptions
}

func (o *orbitDBDocumentStore) Get(ctx context.Context, key string, opts *iface.DocumentStoreGetOptions) ([]interface{}, error) {
	if opts == nil {
		opts = &iface.DocumentStoreGetOptions{}
	}

	hasMultipleTerms := strings.Contains(key, " ")
	if hasMultipleTerms {
		key = strings.ReplaceAll(key, ".", " ")
	}
	if opts.CaseInsensitive {
		key = strings.ToLower(key)
	}

	docIndex, ok := o.Index().(*documentIndex)
	if !ok {
		return nil, fmt.Errorf("unable to cast index to documentIndex")
	}

	documents := []interface{}(nil)

	for _, indexKey := range docIndex.Keys() {
		indexKeyForSearch := indexKey

		if opts.CaseInsensitive {
			indexKeyForSearch = strings.ToLower(indexKeyForSearch)
			if hasMultipleTerms {
				indexKeyForSearch = strings.ReplaceAll(indexKeyForSearch, ".", " ")
			}
		}

		if !opts.PartialMatches {
			if indexKeyForSearch != key {
				continue
			}
		} else if opts.PartialMatches {
			if !strings.Contains(indexKeyForSearch, key) {
				continue
			}
		}

		value := o.Index().Get(indexKey)
		if value == nil {
			return nil, fmt.Errorf("value not found for key %s", indexKey)
		}

		if _, ok := value.([]byte); !ok {
			return nil, fmt.Errorf("invalid type for key %s", indexKey)
		}

		out := o.docOpts.ItemFactory()
		if err := o.docOpts.Unmarshal(value.([]byte), &out); err != nil {
			return nil, fmt.Errorf("unable to unmarshal value for key %s: %w", indexKey, err)
		}

		documents = append(documents, out)
	}

	return documents, nil
}

func (o *orbitDBDocumentStore) Put(ctx context.Context, document interface{}) (operation.Operation, error) {
	key, err := o.docOpts.KeyExtractor(document)
	if err != nil {
		return nil, fmt.Errorf("unable to extract key from value: %w", err)
	}

	data, err := o.docOpts.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal value: %w", err)
	}

	op := operation.NewOperation(&key, "PUT", data)

	e, err := o.AddOperation(ctx, op, nil)
	if err != nil {
		return nil, fmt.Errorf("error while adding operation: %w", err)
	}

	op, err = operation.ParseOperation(e)
	if err != nil {
		return nil, fmt.Errorf("unable to parse newly created entry: %w", err)
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
		return nil, fmt.Errorf("error while adding operation: %w", err)
	}

	op, err = operation.ParseOperation(e)
	if err != nil {
		return nil, fmt.Errorf("unable to parse newly created entry: %w", err)
	}

	return op, nil
}

// PutBatch Add values as multiple operations and returns the latest
func (o *orbitDBDocumentStore) PutBatch(ctx context.Context, values []interface{}) (operation.Operation, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("nothing to add to the store")
	}

	op := operation.Operation(nil)
	var err error
	for _, val := range values {
		op, err = o.Put(ctx, val)
		if err != nil {
			return nil, fmt.Errorf("unable to add data to the store: %w", err)
		}
	}

	return op, nil
}

// PutAll Add values as a single operation and returns it
func (o *orbitDBDocumentStore) PutAll(ctx context.Context, values []interface{}) (operation.Operation, error) {
	toAdd := map[string][]byte{}

	for _, val := range values {
		key, err := o.docOpts.KeyExtractor(val)
		if err != nil {
			return nil, fmt.Errorf("one of the provided documents has no index key")
		}

		data, err := o.docOpts.Marshal(val)
		if err != nil {
			return nil, fmt.Errorf("unable to marshal one of the provided documents")
		}

		toAdd[key] = data
	}

	empty := ""
	op := operation.NewOperationWithDocuments(&empty, "PUTALL", toAdd)

	e, err := o.AddOperation(ctx, op, nil)
	if err != nil {
		return nil, fmt.Errorf("error while adding operation: %w", err)
	}

	op, err = operation.ParseOperation(e)
	if err != nil {
		return nil, fmt.Errorf("unable to parse newly created entry: %w", err)
	}

	return op, nil
}

// Query Finds documents using a filter function
func (o *orbitDBDocumentStore) Query(ctx context.Context, filter func(doc interface{}) (bool, error)) ([]interface{}, error) {
	docIndex, ok := o.Index().(*documentIndex)
	if !ok {
		return nil, fmt.Errorf("unable to cast index to documentIndex")
	}

	documents := []interface{}(nil)
	for _, indexKey := range docIndex.Keys() {
		doc := docIndex.Get(indexKey)
		if doc == nil {
			continue
		}

		value := o.docOpts.ItemFactory()
		if err := o.docOpts.Unmarshal(doc.([]byte), &value); err != nil {
			return nil, fmt.Errorf("unable to unmarshal document: %w", err)
		}

		if ok, err := filter(value); err != nil {
			return nil, fmt.Errorf("error while filtering value: %w", err)
		} else if ok {
			documents = append(documents, value)
		}
	}

	return documents, nil
}

func (o *orbitDBDocumentStore) Type() string {
	return "docstore"
}

func MapKeyExtractor(keyField string) func(obj interface{}) (string, error) {
	return func(obj interface{}) (string, error) {
		mapped, ok := obj.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("can't extract key from something else than a `map[string]interface{}` entry")
		}

		val, ok := mapped[keyField]
		if !ok {
			return "", fmt.Errorf("missing value for field `%s` in entry", keyField)
		}

		key, ok := val.(string)
		if !ok {
			return "", fmt.Errorf("value for field `%s` is not a string", keyField)
		}

		return key, nil
	}
}

func DefaultStoreOptsForMap(keyField string) *iface.CreateDocumentDBOptions {
	return &iface.CreateDocumentDBOptions{
		Marshal:      json.Marshal,
		Unmarshal:    json.Unmarshal,
		KeyExtractor: MapKeyExtractor(keyField),
		ItemFactory:  func() interface{} { return map[string]interface{}{} },
	}
}

// NewOrbitDBDocumentStore Instantiates a new DocumentStore
func NewOrbitDBDocumentStore(ipfs coreapi.CoreAPI, identity *identityprovider.Identity, addr address.Address, options *iface.NewStoreOptions) (iface.Store, error) {
	if options.StoreSpecificOpts == nil {
		options.StoreSpecificOpts = DefaultStoreOptsForMap("_id")
	}

	docOpts, ok := options.StoreSpecificOpts.(*iface.CreateDocumentDBOptions)
	if !ok {
		return nil, fmt.Errorf("invalid type supplied for opts.StoreSpecificOpts")
	}

	if docOpts.Marshal == nil {
		return nil, fmt.Errorf("missing value for option opts.StoreSpecificOpts.Marshal")
	}

	if docOpts.Unmarshal == nil {
		return nil, fmt.Errorf("missing value for option opts.StoreSpecificOpts.Unmarshal")
	}

	if docOpts.ItemFactory == nil {
		return nil, fmt.Errorf("missing value for option opts.StoreSpecificOpts.ItemFactory")
	}

	if docOpts.KeyExtractor == nil {
		return nil, fmt.Errorf("missing value for option opts.StoreSpecificOpts.ExtractKey")
	}

	store := &orbitDBDocumentStore{docOpts: docOpts}
	options.Index = func(_ []byte) iface.StoreIndex { return newDocumentIndex(docOpts) }

	err := store.InitBaseStore(ipfs, identity, addr, options)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize document store: %w", err)
	}

	return store, nil
}

var _ iface.DocumentStore = &orbitDBDocumentStore{}
