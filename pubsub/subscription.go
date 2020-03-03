package pubsub

import (
	"context"
	"fmt"

	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/pubsub/peermonitor"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
	iface "github.com/ipfs/interface-go-ipfs-core"
	p2pcore "github.com/libp2p/go-libp2p-core"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type subscription struct {
	events.EventEmitter
	cancel    context.CancelFunc
	pubSubSub iface.PubSubSubscription
	ipfs      coreapi.CoreAPI
	id        p2pcore.PeerID
}

// NewSubscription Creates a new pub sub subscription
func NewSubscription(ctx context.Context, ipfs coreapi.CoreAPI, topic string, cancel context.CancelFunc) (Subscription, error) {
	pubSubSub, err := ipfs.PubSub().Subscribe(ctx, topic)
	if err != nil {
		return nil, err
	}

	id, err := ipfs.Key().Self(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get id for user")
	}

	s := &subscription{
		ipfs:      ipfs,
		pubSubSub: pubSubSub,
		id:        id.ID(),
	}

	go s.listener(ctx, pubSubSub, topic)
	go s.topicMonitor(ctx, topic)

	return s, nil
}

func (s *subscription) Close() error {
	err := s.pubSubSub.Close()
	if err != nil {
		logger().Error("error while closing subscription", zap.Error(err))
	}

	if s.cancel != nil {
		s.cancel()
	}

	return nil
}

func (s *subscription) topicMonitor(ctx context.Context, topic string) {
	pm := peermonitor.NewPeerMonitor(ctx, s.ipfs, topic, nil)

	go func() {
		for evt := range pm.Subscribe(ctx) {
			switch e := evt.(type) {
			case *peermonitor.EventPeerJoin:
				logger().Debug(fmt.Sprintf("peer %s joined topic %s", e.Peer, topic))
				break

			case *peermonitor.EventPeerLeave:
				logger().Debug(fmt.Sprintf("peer %s left topic %s", e.Peer, topic))
				break
			}

			s.Emit(ctx, evt)
		}
	}()

	pm.Start(ctx)

}

func (s *subscription) listener(ctx context.Context, subSubscription iface.PubSubSubscription, topic string) {
	for {
		msg, err := subSubscription.Next(ctx)
		if err != nil {
			if ctx.Err() == nil {
				logger().Error(fmt.Sprintf("unable to get pub sub message"), zap.Error(err))
			}

			break
		}

		if msg.From() == s.id {
			continue
		}

		msgTopic := msg.Topics()[0]

		if topic != msgTopic {
			logger().Debug("message is from another topic, ignoring")
			continue
		}

		logger().Debug(fmt.Sprintf("got pub sub message from %s", s.id))

		s.Emit(ctx, NewMessageEvent(topic, msg.Data()))
	}
}

var _ Subscription = &subscription{}
