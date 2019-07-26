package pubsub

import (
	"context"
	"fmt"
	"github.com/berty/go-orbit-db/events"
	"github.com/berty/go-orbit-db/ipfs"
	"github.com/berty/go-orbit-db/pubsub/peermonitor"
	iface "github.com/ipfs/interface-go-ipfs-core"
	p2pcore "github.com/libp2p/go-libp2p-core"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type subscription struct {
	events.EventEmitter
	cancel    context.CancelFunc
	pubSubSub iface.PubSubSubscription
	services  ipfs.Services
	id        p2pcore.PeerID
}

func NewSubscription(ctx context.Context, services ipfs.Services, topic string) (Subscription, error) {
	_, cancel := context.WithCancel(ctx)

	pubSubSub, err := services.PubSub().Subscribe(ctx, topic)
	if err != nil {
		return nil, err
	}

	id, err := services.Key().Self(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get id for user")
	}

	s := &subscription{
		services:  services,
		pubSubSub: pubSubSub,
		cancel:    cancel,
		id:        id.ID(),
	}

	go s.listener(ctx, pubSubSub, topic)
	go s.topicMonitor(ctx, topic)

	return s, nil
}

func (s *subscription) Close() error {
	s.cancel()

	return nil
}

func (s *subscription) topicMonitor(ctx context.Context, topic string) {
	pm := peermonitor.NewPeerMonitor(ctx, s.services, topic, nil)
	ch := pm.Subscribe()
	pm.Start(ctx)

	for {
		select {
		case <-ctx.Done():
			return

		case evt := <-ch:
			switch evt.(type) {
			case *peermonitor.EventPeerJoin:
				e := evt.(*peermonitor.EventPeerJoin)
				logger().Debug(fmt.Sprintf("peer %s joined topic %s", e.Peer, topic))
				break

			case *peermonitor.EventPeerLeave:
				e := evt.(*peermonitor.EventPeerLeave)
				logger().Debug(fmt.Sprintf("peer %s left topic %s", e.Peer, topic))
				break
			}

			s.Emit(evt)
		}
	}
}

func (s *subscription) listener(ctx context.Context, subSubscription iface.PubSubSubscription, topic string) {
	for {
		msg, err := subSubscription.Next(ctx)
		if err != nil {
			logger().Error(fmt.Sprintf("unable to get pub sub message"), zap.Error(err))
			break
		}

		logger().Debug(fmt.Sprintf("got pub sub message from %s", s.id))

		if msg.From() == s.id {
			logger().Debug(fmt.Sprintf("message sender is self (%s), ignoring", s.id))
			continue
		}

		msgTopic := msg.Topics()[0]

		if topic != msgTopic {
			logger().Debug("message is from another topic, ignoring")
			continue
		}

		s.Emit(NewMessageEvent(topic, msg.Data()))
	}
}

var _ Subscription = &subscription{}
