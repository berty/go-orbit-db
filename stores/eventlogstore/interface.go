package eventlogstore

import (
	"context"
	"github.com/berty/go-orbit-db/stores"
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
	//All()
	Add(ctx context.Context, data []byte) error
	Get(ctx context.Context, cid cid.Cid) ([]byte, error)
	Stream(ctx context.Context, resultChan chan []byte, options *StreamOptions) error
}
