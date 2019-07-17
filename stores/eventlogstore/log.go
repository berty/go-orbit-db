package eventlogstore

import (
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/identityprovider"
	"context"
	"github.com/berty/go-orbit-db/address"
	"github.com/berty/go-orbit-db/ipfs"
	"github.com/berty/go-orbit-db/stores"
	"github.com/berty/go-orbit-db/stores/basestore"
	"github.com/berty/go-orbit-db/stores/operation"
	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
)

type orbitDBEventLogStore struct {
	basestore.BaseStore
}

func (o *orbitDBEventLogStore) List(ctx context.Context, options *StreamOptions) ([]operation.Operation, error) {
	var err error
	var operations []operation.Operation
	c := make(chan operation.Operation)

	go func () {
		err = o.Stream(ctx, c, options)
	}()
	for op := range c {
		operations = append(operations, op)
		if err != nil {
			return nil, errors.Wrap(err, "unable to list operations")
		}
	}

	if err != nil {
		return nil, errors.Wrap(err, "unable to list operations")
	}

	return operations, nil
}

func (o *orbitDBEventLogStore) All() []*entry.Entry {
	return o.OpLog().Values().Slice()
}

func (o *orbitDBEventLogStore) Add(ctx context.Context, value []byte) (operation.Operation, error) {
	op := operation.NewOperation(nil, "ADD", value)

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

func (o *orbitDBEventLogStore) Get(ctx context.Context, cid cid.Cid) (operation.Operation, error) {
	stream := make(chan operation.Operation, 1)
	one := 1

	err := o.Stream(ctx, stream, &StreamOptions{GTE: &cid, Amount: &one})
	if err != nil {
		return nil, errors.Wrap(err, "unable to open stream")
	}

	select {
	case value := <-stream:
		return value, nil
	case <-ctx.Done():
		return nil, errors.New("context deadline exceeded")
	}
}

func (o *orbitDBEventLogStore) Stream(ctx context.Context, resultChan chan operation.Operation, options *StreamOptions) error {
	messages, err := o.query(options)
	if err != nil {
		return errors.Wrap(err, "unable to fetch query results")
	}

	for _, message := range messages {
		op, err := operation.ParseOperation(message)
		if err != nil {
			return errors.Wrap(err, "unable to parse operation")
		}

		resultChan <- op
	}

	close(resultChan)

	return nil
}

func (o *orbitDBEventLogStore) query(options *StreamOptions) ([]*entry.Entry, error) {
	if options == nil {
		options = &StreamOptions{}
	}

	uncastedEvents := o.Index().Get("")
	if uncastedEvents == nil {
		return nil, nil
	}

	events, ok := o.Index().Get("").([]*entry.Entry)
	if !ok {
		return nil, errors.New("unable to cast index to entries")
	}

	amount := 1
	if options.Amount != nil {
		if *options.Amount  == 0 {
			amount = 1
		} else if *options.Amount > -1 {
			amount = *options.Amount
		} else {
			amount = len(events)
		}
	}

	var c cid.Cid
	if options.GT != nil || options.GTE != nil {
		// Greater than case

		if options.GT != nil {
			c = *options.GT
		} else {
			c = *options.GTE
		}

		return o.read(events, c, amount, options.GTE != nil), nil
	}

	if options.LT != nil {
		c = *options.LT
	} else if options.LTE != nil {
		c = *options.LTE
	}

	// Reversing events
	for i := len(events)/2 - 1; i >= 0; i-- {
		opp := len(events) - 1 - i
		events[i], events[opp] = events[opp], events[i]
	}

	// Lower than and lastN case, search latest first by reversing the sequence
	result := o.read(events, c, amount, options.LTE != nil || options.LT == nil)

	// Reversing result
	for i := len(result)/2 - 1; i >= 0; i-- {
		opp := len(result) - 1 - i
		result[i], result[opp] = result[opp], result[i]
	}

	return result, nil
}

func (o *orbitDBEventLogStore) read(ops []*entry.Entry, hash cid.Cid, amount int, inclusive bool) []*entry.Entry {
	// Find the index of the gt/lt hash, or start from the beginning of the array if not found
	startIndex := 0
	for i, e := range ops {
		if e.Hash.String() == hash.String() {
			startIndex = i
			break
		}
	}

	// If gte/lte is set, we include the given hash, if not, start from the next element
	if !inclusive {
		startIndex++
	}

	var result []*entry.Entry

	// Slice the array to its requested size
	for i, e := range ops {
		if i < startIndex {
			continue
		}

		if amount == 0 {
			break
		}

		result = append(result, e)
		amount--
	}

	return result
}

func (o *orbitDBEventLogStore) Type() string {
	return "eventlog"
}

func init() {
	stores.RegisterStore("eventlog", newOrbitDBEventLogStore)
}

func newOrbitDBEventLogStore(ctx context.Context, services ipfs.Services, identity *identityprovider.Identity, addr address.Address, options *stores.NewStoreOptions) (i stores.Interface, e error) {
	store := &orbitDBEventLogStore{}
	options.Index = NewEventIndex

	err := store.InitBaseStore(ctx, services, identity, addr, options)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize base store")
	}

	return store, nil
}

var _ OrbitDBEventLogStore = &orbitDBEventLogStore{}
