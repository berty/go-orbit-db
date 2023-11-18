package eventlogstore

import (
	"context"
	"fmt"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-orbit-db/address"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores/basestore"
	"berty.tech/go-orbit-db/stores/operation"
	coreapi "github.com/ipfs/boxo/coreiface"
	cid "github.com/ipfs/go-cid"
)

type orbitDBEventLogStore struct {
	basestore.BaseStore
}

func (o *orbitDBEventLogStore) List(ctx context.Context, options *iface.StreamOptions) ([]operation.Operation, error) {
	var operations []operation.Operation
	c := make(chan operation.Operation)

	go func() {
		_ = o.Stream(ctx, c, options)
	}()
	for op := range c {
		operations = append(operations, op)
	}

	return operations, nil
}

func (o *orbitDBEventLogStore) Add(ctx context.Context, value []byte) (operation.Operation, error) {
	op := operation.NewOperation(nil, "ADD", value)

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

func (o *orbitDBEventLogStore) Get(ctx context.Context, cid cid.Cid) (operation.Operation, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errChan := make(chan error)

	stream := make(chan operation.Operation)
	one := 1

	go func() {
		if err := o.Stream(ctx, stream, &iface.StreamOptions{GTE: &cid, Amount: &one}); err != nil {
			errChan <- fmt.Errorf("unable to open stream: %w", err)
			cancel()
			return
		}
	}()

	select {
	case value, ok := <-stream:
		cancel()
		if ok {
			return value, nil
		}
		return nil, fmt.Errorf("channel read failed")

	case err := <-errChan:
		return nil, err

	case <-ctx.Done():
		return nil, fmt.Errorf("context deadline exceeded")
	}
}

func (o *orbitDBEventLogStore) Stream(ctx context.Context, resultChan chan operation.Operation, options *iface.StreamOptions) error {
	defer close(resultChan)
	messages, err := o.query(options)
	if err != nil {
		return fmt.Errorf("unable to fetch query results: %w", err)
	}

	for _, message := range messages {
		op, err := operation.ParseOperation(message)
		if err != nil {
			return fmt.Errorf("unable to parse operation: %w", err)
		}

		resultChan <- op
	}

	return nil
}

func (o *orbitDBEventLogStore) query(options *iface.StreamOptions) ([]ipfslog.Entry, error) {
	if options == nil {
		options = &iface.StreamOptions{}
	}

	uncastedEvents := o.Index().Get("")
	if uncastedEvents == nil {
		return nil, nil
	}

	events, ok := o.Index().Get("").([]ipfslog.Entry)
	if !ok {
		return nil, fmt.Errorf("unable to cast index to entries")
	}

	amount := 1
	if options.Amount != nil {
		if *options.Amount == 0 {
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

func (o *orbitDBEventLogStore) read(ops []ipfslog.Entry, hash cid.Cid, amount int, inclusive bool) []ipfslog.Entry {
	// Find the index of the gt/lt hash, or start from the beginning of the array if not found
	startIndex := 0
	for i, e := range ops {
		if e.GetHash().String() == hash.String() {
			startIndex = i
			break
		}
	}

	// If gte/lte is set, we include the given hash, if not, start from the next element
	if !inclusive {
		startIndex++
	}

	var result []ipfslog.Entry

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

// NewOrbitDBEventLogStore Instantiates a new EventLogStore
func NewOrbitDBEventLogStore(ipfs coreapi.CoreAPI, identity *identityprovider.Identity, addr address.Address, options *iface.NewStoreOptions) (i iface.Store, e error) {

	store := &orbitDBEventLogStore{}
	options.Index = NewEventIndex

	err := store.InitBaseStore(ipfs, identity, addr, options)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize base store: %w", err)
	}

	return store, nil
}

var _ iface.EventLogStore = &orbitDBEventLogStore{}
