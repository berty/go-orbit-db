package baseorbitdb

import (
	"context"
	"fmt"

	ipfslog "github.com/stateless-minds/go-ipfs-log"
	"github.com/stateless-minds/go-orbit-db/iface"
)

func (o *orbitDB) handleEventExchangeHeads(ctx context.Context, e *iface.MessageExchangeHeads, store iface.Store) error {
	untypedHeads := make([]ipfslog.Entry, len(e.Heads))
	for i, h := range e.Heads {
		untypedHeads[i] = h
	}

	o.logger.Debug(fmt.Sprintf("%s: Received %d heads for '%s':", o.PeerID().String(), len(untypedHeads), e.Address))

	if len(untypedHeads) > 0 {
		if err := store.Sync(ctx, untypedHeads); err != nil {
			return fmt.Errorf("unable to sync heads: %w", err)
		}
	}

	return nil
}
