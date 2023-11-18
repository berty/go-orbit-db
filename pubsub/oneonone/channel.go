package oneonone

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	coreapi "github.com/ipfs/boxo/coreiface"
	"github.com/ipfs/boxo/coreiface/options"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"

	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/pubsub"
)

const (
	PROTOCOL    = "ipfs-pubsub-direct-channel/v1"
	HelloPacket = "hello"
)

type channel struct {
	ctx    context.Context
	cancel context.CancelFunc
	id     string
	sub    coreapi.PubSubSubscription
}

type channels struct {
	subs   map[peer.ID]*channel
	muSubs sync.RWMutex

	selfID  peer.ID
	emitter iface.DirectChannelEmitter
	ctx     context.Context
	cancel  context.CancelFunc
	ipfs    coreapi.CoreAPI
	logger  *zap.Logger
}

func (c *channels) Connect(ctx context.Context, target peer.ID) error {
	id := c.getChannelID(target)

	c.muSubs.Lock()
	if _, ok := c.subs[target]; !ok {
		c.logger.Debug("subscribing to", zap.String("topic", id))

		sub, err := c.ipfs.PubSub().Subscribe(ctx, id, options.PubSub.Discover(true))
		if err != nil {
			c.muSubs.Unlock()
			return fmt.Errorf("unable to subscribe to pubsub: %w", err)
		}

		ctx, cancel := context.WithCancel(ctx)

		c.subs[target] = &channel{
			ctx:    ctx,
			cancel: cancel,
			sub:    sub,
			id:     id,
		}
		go func() {
			c.monitorTopic(ctx, sub, target)

			// if monitor topic is done, remove target from cache
			c.muSubs.Lock()
			delete(c.subs, target)
			c.muSubs.Unlock()
		}()
	}
	c.muSubs.Unlock()

	// try to run a connect first.
	// if enable, it will use ipfs discovery in the background
	if err := c.ipfs.Swarm().Connect(ctx, peer.AddrInfo{ID: target}); err != nil {
		c.logger.Warn("unable to connect to remote peer", zap.String("peer", target.String()))
	}

	return c.waitForPeers(ctx, target, id)
}

func (c *channels) Send(ctx context.Context, p peer.ID, head []byte) error {
	var id string
	c.muSubs.RLock()
	if ch, ok := c.subs[p]; ok {
		id = ch.id
	} else {
		id = c.getChannelID(p)
	}
	c.muSubs.RUnlock()

	err := c.ipfs.PubSub().Publish(ctx, id, head)
	if err != nil {
		return fmt.Errorf("unable to publish data on pubsub: %w", err)
	}

	return nil
}

func (c *channels) waitForPeers(ctx context.Context, otherPeer peer.ID, channelID string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}

		peers, err := c.ipfs.PubSub().Peers(ctx, options.PubSub.Topic(channelID))
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
	}
}

func (c *channels) getChannelID(p peer.ID) string {
	channelIDPeers := []string{c.selfID.String(), p.String()}
	sort.Slice(channelIDPeers, func(i, j int) bool {
		return strings.Compare(channelIDPeers[i], channelIDPeers[j]) < 0
	})
	// ID of the channel is "<peer1 id>/<peer 2 id>""
	return fmt.Sprintf("/%s/%s", PROTOCOL, strings.Join(channelIDPeers, "/"))
}

func (c *channels) monitorTopic(ctx context.Context, sub coreapi.PubSubSubscription, p peer.ID) {
	for {
		msg, err := sub.Next(ctx)
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

		if err := c.emitter.Emit(pubsub.NewEventPayload(msg.Data(), p)); err != nil {
			c.logger.Warn("unable to emit event payload", zap.Error(err))
		}
	}
}

func (c *channels) Close() error {
	c.cancel()

	c.muSubs.Lock()
	for _, ch := range c.subs {
		_ = ch.sub.Close()
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
			return nil, fmt.Errorf("unable to get key for self: %w", err)
		}

		ch := &channels{
			emitter: emitter,
			subs:    make(map[peer.ID]*channel),
			ctx:     ctx,
			selfID:  selfKey.ID(),
			cancel:  cancel,
			ipfs:    ipfs,
			logger:  opts.Logger,
		}

		return ch, nil
	}
}

var _ iface.DirectChannel = &channels{}
