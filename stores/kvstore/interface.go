package kvstore

import (
	"context"
	"github.com/berty/go-orbit-db/stores"
	"github.com/berty/go-orbit-db/stores/operation"
)

type OrbitDBKeyValue interface {
	stores.Interface
	All() map[string][]byte
	Put(ctx context.Context, key string, value []byte) (operation.Operation, error)
	Delete(ctx context.Context, key string) (operation.Operation, error)
	Get(ctx context.Context, key string) ([]byte, error)
}
