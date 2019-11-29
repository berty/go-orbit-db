package oneonone

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"berty.tech/go-orbit-db/events"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
	iface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/options"
	p2pcore "github.com/libp2p/go-libp2p-core"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type channel struct {
	events.EventEmitter
	id         string
	receiverID p2pcore.PeerID
	senderID   p2pcore.PeerID
	ipfs       coreapi.CoreAPI
	peers      []p2pcore.PeerID
	sub        iface.PubSubSubscription
	done       bool
	lock       sync.RWMutex
}

func (c *channel) ID() string {
	c.lock.RLock()
	id := c.id
	c.lock.RUnlock()

	return id
}

func (c *channel) Peers() []p2pcore.PeerID {
	c.lock.RLock()
	peers := c.peers
	c.lock.RUnlock()

	return peers

}

func (c *channel) Connect(ctx context.Context) error {
	c.lock.RLock()
	receiverID := c.receiverID
	c.lock.RUnlock()

	err := c.waitForPeers(ctx, []p2pcore.PeerID{receiverID})
	if err != nil {
		return errors.Wrap(err, "unable to wait for peers")
	}

	return nil
}

func (c *channel) IPFS() iface.CoreAPI {
	c.lock.RLock()
	ipfs := c.ipfs
	c.lock.RUnlock()

	return ipfs
}

func (c *channel) waitForPeers(ctx context.Context, peersToWait []p2pcore.PeerID) error {
	id := c.ID()
	ipfs := c.IPFS()

	peers, err := ipfs.PubSub().Peers(ctx, options.PubSub.Topic(id))
	if err != nil {
		logger().Error("failed to get peers on pub sub")
		return err
	}

	foundAllPeers := true
	for _, p1 := range peersToWait {
		foundPeer := false
		for _, p2 := range peers {
			if p1 == p2 {
				foundPeer = true
				break
			}
		}

		if !foundPeer {
			foundAllPeers = false
			break
		}
	}

	if foundAllPeers {
		return nil
	}

	logger().Debug("Failed to get peer on pub sub retrying...")
	<-time.After(100 * time.Millisecond)

	return c.waitForPeers(ctx, peersToWait)
}

func (c *channel) Send(ctx context.Context, data []byte) error {
	id := c.ID()
	ipfs := c.IPFS()

	err := ipfs.PubSub().Publish(ctx, id, data)
	if err != nil {
		return errors.Wrap(err, "unable to publish data on pubsub")
	}

	return nil
}

func (c *channel) Done() bool {
	c.lock.RLock()
	done := c.done
	c.lock.RUnlock()

	return done
}

func (c *channel) Close() error {
	c.UnsubscribeAll()
	_ = c.sub.Close() // TODO: handle errors

	c.lock.Lock()
	c.done = true
	c.lock.Unlock()

	return nil
}

func (c *channel) SenderID() string {
	c.lock.RLock()
	senderID := c.senderID.String()
	c.lock.RUnlock()

	return senderID
}

// NewChannel Creates a new pubsub topic for communication between two peers
func NewChannel(ctx context.Context, ipfs coreapi.CoreAPI, pid p2pcore.PeerID) (Channel, error) {
	selfKey, err := ipfs.Key().Self(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get key for self")
	}

	channelIDPeers := []string{pid.String(), selfKey.ID().String()}
	sort.Strings(channelIDPeers)

	// ID of the channel is "<peer1 id>/<peer 2 id>""
	channelID := fmt.Sprintf("/%s/%s", PROTOCOL, strings.Join(channelIDPeers, "/"))

	logger().Debug(fmt.Sprintf("subscribing to %s", channelID))

	sub, err := ipfs.PubSub().Subscribe(ctx, channelID)

	ch := &channel{
		id:         channelID,
		ipfs:       ipfs,
		receiverID: pid,
		senderID:   selfKey.ID(),
		peers:      []p2pcore.PeerID{pid, selfKey.ID()},
		sub:        sub,
	}

	if err != nil {
		return nil, errors.Wrap(err, "unable to subscribe to pubsub")
	}

	go func() {
		for {
			if ctx.Err() != nil || ch.Done() {
				return
			}

			msg, err := sub.Next(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}

				logger().Error("unable to get pub sub message", zap.Error(err))
				continue
			}

			// Make sure the message is coming from the correct peer
			// Filter out all messages that didn't come from the second peer
			if msg.From().String() == ch.SenderID() {
				continue
			}

			ch.Emit(NewEventMessage(msg.Data()))
		}
	}()

	return ch, nil
}

var _ Channel = &channel{}
