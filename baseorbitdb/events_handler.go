package baseorbitdb

import (
	"context"
	"encoding/json"
	"fmt"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/enc"
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores"
)

func (o *orbitDB) handleEventExchangeHeads(ctx context.Context, e *EventExchangeHeads, sharedKey enc.SharedKey) error {
	message := e.Message

	var (
		address  string
		rawHeads []byte
	)

	if sharedKey != nil {
		// open address
		rawAddress, err := sharedKey.Open(message.Address)
		if err != nil {
			return fmt.Errorf("unable to decrypt address: %w", err)
		}
		address = string(rawAddress)

		// open heads
		if rawHeads, err = sharedKey.Open(message.Heads); err != nil {
			return fmt.Errorf("unable to decrypt heads: %w", err)
		}
	} else {
		address = string(message.Address)
		rawHeads = message.Heads
	}

	store, ok := o.getStore(address)
	if !ok {
		return fmt.Errorf("receiving heads from unknown store")
	}

	heads := []*entry.Entry{}
	if err := json.Unmarshal(rawHeads, &heads); err != nil {
		return fmt.Errorf("unable to parse heads: %w", err)
	}

	untypedHeads := make([]ipfslog.Entry, len(heads))
	for i, h := range heads {
		untypedHeads[i] = h
	}

	o.logger.Debug(fmt.Sprintf("%s: Received %d heads for '%s':", o.PeerID().String(), len(heads), address))

	if len(heads) > 0 {
		if err := store.Sync(ctx, untypedHeads); err != nil {
			return fmt.Errorf("unable to sync heads: %w", err)
		}
	}

	return nil
}

func (o *orbitDB) handleEventWrite(ctx context.Context, e *stores.EventWrite, store Store, topic iface.PubSubTopic) error {
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
			headsBytes, err := json.Marshal(e.Heads)
			if err != nil {
				return fmt.Errorf("unable to serialize heads %w", err)
			}

			if key := store.SharedKey(); key != nil {
				headsBytes, err = key.Seal(headsBytes)
				if err != nil {
					return fmt.Errorf("unable to encrypt heads %w", err)
				}
			}

			err = topic.Publish(ctx, headsBytes)
			if err != nil {
				return fmt.Errorf("unable to publish message on pubsub %w", err)
			}

			o.logger.Debug("stores.write event: published event on pub sub")
		}
	}

	return nil
}
