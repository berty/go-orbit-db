package oneonone

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	coreapi "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/options"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/pubsub"
)

const (
	PROTOCOL    = "ipfs-pubsub-direct-channel/v1"
	HelloPacket = "hello"
)

type channel struct {
	ccpeers map[peer.ID]chan struct{}
	subs    map[peer.ID]coreapi.PubSubSubscription
	muSubs  sync.Mutex

	selfID  peer.ID
	emitter iface.DirectChannelEmitter
	ctx     context.Context
	cancel  context.CancelFunc
	ipfs    coreapi.CoreAPI
	sub     coreapi.PubSubSubscription
	logger  *zap.Logger
}

func (c *channel) Connect(ctx context.Context, target peer.ID) error {
	channelID := c.getOurChannelID(target)

	c.muSubs.Lock()
	if _, ok := c.subs[target]; !ok {
		c.logger.Debug(fmt.Sprintf("subscribing to %s", channelID))

		sub, err := c.ipfs.PubSub().Subscribe(ctx, channelID)
		if err != nil {
			c.muSubs.Unlock()
			return errors.Wrap(err, "unable to subscribe to pubsub")
		}

		theirChannelID := c.getTheirChannelID(target)

		if err = c.ipfs.PubSub().Publish(ctx, theirChannelID, []byte(HelloPacket)); err != nil {
			c.muSubs.Unlock()
			return errors.Wrap(err, "unable to publish to pubsub")
		}

		go c.monitorTopic(sub, target)

		c.subs[target] = sub
	}
	c.muSubs.Unlock()

	// @FIXME(gfanton): this is very bad
	c.waitForPeers(ctx, target, channelID)
	return nil
}

func (c *channel) sendHelloPacket(ctx context.Context, p peer.ID) error {
	theirChannelID := c.getTheirChannelID(p)
	if err := c.ipfs.PubSub().Publish(ctx, theirChannelID, []byte(HelloPacket)); err != nil {
		return errors.Wrap(err, "unable to publish to pubsub")
	}

	return nil
}

func (c *channel) Send(ctx context.Context, p peer.ID, data []byte) error {
	channelid := c.getOurChannelID(p)
	err := c.ipfs.PubSub().Publish(ctx, channelid, data)
	if err != nil {
		return errors.Wrap(err, "unable to publish data on pubsub")
	}

	return nil
}

func (c *channel) waitForPeers(ctx context.Context, otherPeer peer.ID, channelID string) error {
	fmt.Printf("waiting for other peer: %s\n", otherPeer.String())
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}

		peers, err := c.ipfs.PubSub().Peers(ctx, options.PubSub.Topic(channelID))
		if err != nil {
			fmt.Printf("failed to get peer\n")
			c.logger.Error("failed to get peers on pub sub")
			return err
		}

		for _, p := range peers {
			if p == otherPeer {
				fmt.Printf("found other peer: %s\n", otherPeer.String())
				return nil
			}
		}

		c.logger.Debug("Failed to get peer on pub sub retrying...")
	}
}

func (c *channel) getTheirChannelID(p peer.ID) string {
	channelIDPeers := []string{c.selfID.String(), p.String()}

	// ID of the channel is "<peer1 id>/<peer 2 id>""
	return fmt.Sprintf("/%s/%s", PROTOCOL, strings.Join(channelIDPeers, "/"))
}

func (c *channel) getOurChannelID(p peer.ID) string {
	channelIDPeers := []string{p.String(), c.selfID.String()}

	// ID of the channel is "<peer1 id>/<peer 2 id>""
	return fmt.Sprintf("/%s/%s", PROTOCOL, strings.Join(channelIDPeers, "/"))
}

func (c *channel) monitorTopic(sub coreapi.PubSubSubscription, p peer.ID) {
	for {
		msg, err := sub.Next(c.ctx)
		switch err {
		case nil:
		case context.Canceled, context.DeadlineExceeded:
			c.logger.Debug("closing topic monitor", zap.String("remote", p.String()))
			return
		default:
			c.logger.Error("unable to get pub sub message", zap.Error(err))
			return
		}

		// Make sure the message is coming from the correct peer
		// Filter out all messages that didn't come from the second peer
		if msg.From().String() == c.selfID.String() {
			continue
		}

		// skip hello packet
		if bytes.Equal(msg.Data(), []byte(HelloPacket)) {
			continue
		}

		if err := c.emitter.Emit(pubsub.NewEventPayload(msg.Data())); err != nil {
			c.logger.Warn("unable to emit event payload", zap.Error(err))
		}
	}
}

func (c *channel) Close() error {
	c.cancel()

	c.muSubs.Lock()
	for _, sub := range c.subs {
		_ = sub.Close()
	}
	c.muSubs.Unlock()

	_ = c.emitter.Close()

	return nil
}

// NewChannel Creates a new pubsub topic for communication between two peers
func NewChannelFactory(ipfs coreapi.CoreAPI) iface.DirectChannelFactory {
	return func(ctx context.Context, emitter iface.DirectChannelEmitter, opts *iface.DirectChannelOptions) (iface.DirectChannel, error) {
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

		ch := &channel{
			emitter: emitter,
			subs:    make(map[peer.ID]coreapi.PubSubSubscription),
			ctx:     ctx,
			selfID:  selfKey.ID(),
			cancel:  cancel,
			ipfs:    ipfs,
			logger:  opts.Logger,
		}

		return ch, nil
	}
}

var _ iface.DirectChannel = &channel{}
