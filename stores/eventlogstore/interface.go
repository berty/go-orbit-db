package eventlogstore

import (
	"context"
	"github.com/berty/go-orbit-db/stores"
	"github.com/berty/go-orbit-db/stores/operation"
	"github.com/ipfs/go-cid"
)

type StreamOptions struct {
	GT     *cid.Cid
	GTE    *cid.Cid
	LT     *cid.Cid
	LTE    *cid.Cid
	Amount *int
}

type OrbitDBEventLogStore interface {
	stores.Interface
	Add(ctx context.Context, data []byte) (operation.Operation, error)
	Get(ctx context.Context, cid cid.Cid) (operation.Operation, error)
	Stream(ctx context.Context, resultChan chan operation.Operation, options *StreamOptions) error
	List(ctx context.Context, options *StreamOptions) ([]operation.Operation, error)
}
