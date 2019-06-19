package pubsub

import (
	"context"
	"github.com/berty/go-orbit-db/pubsub/peermonitor"
	iface "github.com/ipfs/interface-go-ipfs-core"
)

type subscription struct {
	messageChan chan iface.PubSubMessage
	peerChan    chan *peermonitor.Event
	cancel      context.CancelFunc
}

func (s *subscription) MessageChan() chan iface.PubSubMessage {
	return s.messageChan
}

func (s *subscription) PeerChan() chan *peermonitor.Event {
	return s.peerChan
}

func NewSubscription(ctx context.Context) Subscription {
	_, cancel := context.WithCancel(ctx)

	return &subscription{
		messageChan: make(chan iface.PubSubMessage),
		peerChan:    make(chan *peermonitor.Event),
		cancel:      cancel,
	}
}

func (s *subscription) Close() error {
	s.cancel()

	return nil
}

var _ Subscription = &subscription{}
