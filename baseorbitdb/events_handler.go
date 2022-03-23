package baseorbitdb

import (
	"context"
	"fmt"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores"
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

func (o *orbitDB) handleEventWrite(ctx context.Context, e *stores.EventWrite, topic iface.PubSubTopic, store Store) error {
	o.logger.Debug("received stores.write event")
	if len(e.Heads) == 0 {
		return fmt.Errorf("'heads' are not defined")
	}

	if topic != nil {
		peer, err := topic.Peers(ctx)
		if err != nil {
			return fmt.Errorf("unable to get topic peers: %w", err)
		}

		if len(peer) > 0 {
			entries := make([]*entry.Entry, len(e.Heads))
			for i, head := range e.Heads {
				if entry, ok := head.(*entry.Entry); ok {
					entries[i] = entry
				} else {
					return fmt.Errorf("unable to unwrap entry")
				}
			}

			msg := &iface.MessageExchangeHeads{
				Address: store.Address().String(),
				Heads:   entries,
			}

			payload, err := o.messageMarshaler.Marshal(msg)
			if err != nil {
				return fmt.Errorf("unable to serialize heads %w", err)
			}

			err = topic.Publish(ctx, payload)
			if err != nil {
				return fmt.Errorf("unable to publish message on pubsub %w", err)
			}

			o.logger.Debug("stores.write event: published event on pub sub")
		}
	}

	return nil
}
