package kvstore

import (
	"context"
	"github.com/berty/go-orbit-db/stores"
)

type OrbitDBKeyValue interface {
	stores.Interface
	All() map[string][]byte
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	Get(ctx context.Context, key string) ([]byte, error)
}
