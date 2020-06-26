package oneonone

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	coreapi "github.com/ipfs/interface-go-ipfs-core"
	ipfsIface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/options"
	p2pcore "github.com/libp2p/go-libp2p-core"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/pubsub"
)

const PROTOCOL = "ipfs-pubsub-direct-channel/v1"

type channel struct {
	events.EventEmitter
	ctx        context.Context
	cancel     context.CancelFunc
	id         string
	receiverID p2pcore.PeerID
	ipfs       coreapi.CoreAPI
	sub        ipfsIface.PubSubSubscription
	logger     *zap.Logger
}

func (c *channel) Connect(ctx context.Context) error {
	err := c.waitForPeers(ctx, c.receiverID)
	if err != nil {
		return errors.Wrap(err, "unable to wait for peers")
	}

	return nil
}

func (c *channel) waitForPeers(ctx context.Context, otherPeer p2pcore.PeerID) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		peers, err := c.ipfs.PubSub().Peers(ctx, options.PubSub.Topic(c.id))
		if err != nil {
			c.logger.Error("failed to get peers on pub sub")
			return err
		}

		for _, p := range peers {
			if p == otherPeer {
				return nil
			}
		}

		c.logger.Debug("Failed to get peer on pub sub retrying...")
		<-time.After(100 * time.Millisecond)
	}
}

func (c *channel) Send(ctx context.Context, data []byte) error {
	err := c.ipfs.PubSub().Publish(ctx, c.id, data)
	if err != nil {
		return errors.Wrap(err, "unable to publish data on pubsub")
	}

	return nil
}

func (c *channel) Close() error {
	c.cancel()

	c.UnsubscribeAll()
	_ = c.sub.Close() // TODO: handle errors

	return nil
}

// NewChannel Creates a new pubsub topic for communication between two peers
func NewChannelFactory(ipfs coreapi.CoreAPI) iface.DirectChannelFactory {
	return func(ctx context.Context, pid p2pcore.PeerID, opts *iface.DirectChannelOptions) (iface.DirectChannel, error) {
		ctx, cancel := context.WithCancel(ctx)

		if opts == nil {
			opts = &iface.DirectChannelOptions{}
		}

		if opts.Logger == nil {
			opts.Logger = zap.NewNop()
		}

		selfKey, err := ipfs.Key().Self(ctx)
		if err != nil {
			cancel()
			return nil, errors.Wrap(err, "unable to get key for self")
		}

		channelIDPeers := []string{pid.String(), selfKey.ID().String()}
		sort.Strings(channelIDPeers)

		// ID of the channel is "<peer1 id>/<peer 2 id>""
		channelID := fmt.Sprintf("/%s/%s", PROTOCOL, strings.Join(channelIDPeers, "/"))

		opts.Logger.Debug(fmt.Sprintf("subscribing to %s", channelID))

		sub, err := ipfs.PubSub().Subscribe(ctx, channelID)
		if err != nil {
			cancel()
			return nil, errors.Wrap(err, "unable to subscribe to pubsub")
		}

		ch := &channel{
			ctx:        ctx,
			cancel:     cancel,
			id:         channelID,
			ipfs:       ipfs,
			receiverID: pid,
			sub:        sub,
			logger:     opts.Logger,
		}

		go func() {
			defer cancel()

			for {
				if ctx.Err() != nil {
					return
				}

				msg, err := sub.Next(ctx)
				if ctx.Err() != nil {
					return
				}

				if err != nil {
					ch.logger.Error("unable to get pub sub message", zap.Error(err))
					continue
				}

				// Make sure the message is coming from the correct peer
				// Filter out all messages that didn't come from the second peer
				if msg.From().String() == selfKey.ID().String() {
					continue
				}

				ch.Emit(ctx, pubsub.NewEventPayload(msg.Data()))
			}
		}()

		return ch, nil
	}
}

var _ iface.DirectChannel = &channel{}
