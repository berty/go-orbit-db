package baseorbitdb

import (
	"context"
	"encoding/json"
	"fmt"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/enc"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores"
	"go.uber.org/zap"
)

func (o *orbitDB) handleEventPubSubPayload(ctx context.Context, e *iface.EventPubSubPayload, sharedKey enc.SharedKey, topic iface.PubSubTopic) error {
	heads := &exchangedHeads{}
	payload := e.Payload

	if sharedKey != nil {
		var err error

		payload, err = sharedKey.Open(payload)
		if err != nil {
			return fmt.Errorf("unable to decrypt payload: %w", err)
		}
	}

	err := json.Unmarshal(payload, &heads)
	if err != nil {
		o.logger.Error("unable to unmarshal heads", zap.Error(err))
	}

	o.logger.Debug(fmt.Sprintf("%s: Received %d heads for '%s' from %s:", o.PeerID().String(), len(heads.Heads), heads.Address, e.From))
	store, ok := o.getStore(heads.Address)

	if !ok {
		return fmt.Errorf("heads from unknown store, skipping")
	}

	if len(heads.Heads) > 0 {
		untypedHeads := make([]ipfslog.Entry, len(heads.Heads))
		for i := range heads.Heads {
			untypedHeads[i] = heads.Heads[i]
		}

		if err := store.Sync(ctx, untypedHeads); err != nil {
			return fmt.Errorf("unable to sync heads: %w", err)
		}

		peers, err := topic.Peers(ctx)
		if err != nil {
			o.logger.Debug(fmt.Sprintf("Error while getting peers for topic  %s:", topic.Topic()))
		}
		if heads.ttl > 0 {
			for _, peer := range peers {
				if e.From == peer {
					continue
				}
				//o.logger.Debug("sharing heads",
				//	zap.String("to > ", peer.String()),
				//	zap.String("from < ", e.From.String()),
				//	zap.String("store ", store.Address().String()),
				//)
				o.sendHeads(ctx, peer, store, heads.Heads, heads.ttl-1)
			}
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
