package pubsub

import (
	"context"
	"errors"
	"github.com/berty/go-orbit-db/ipfs"
	"github.com/berty/go-orbit-db/pubsub/peermonitor"
	"github.com/libp2p/go-libp2p-core/peer"
)

type pubSub struct {
	ipfs          ipfs.Services
	id            peer.ID
	subscriptions map[string]Subscription
}

func NewPubSub(is ipfs.Services, id peer.ID) (Interface, error) {
	if is == nil {
		return nil, errors.New("ipfs services are not defined")
	}

	ps := is.PubSub()

	if ps == nil {
		return nil, errors.New("pubsub service is not provided by the current ipfs instance")
	}

	return &pubSub{
		ipfs: is,
		id:   id,
	}, nil
}

func (p *pubSub) Subscribe(ctx context.Context, topic string) (Subscription, error) {
	sub, ok := p.subscriptions[topic]
	if ok {
		return sub, errors.New("already subscribed")
	}

	ipfsPubSubSub, err := p.ipfs.PubSub().Subscribe(ctx, topic)
	if err != nil {
		return nil, err
	}

	s := NewSubscription(ctx)

	pm := peermonitor.NewPeerMonitor(ctx, p.ipfs.PubSub(), topic, s.PeerChan(), nil)

	p.subscriptions[topic] = s

	go func() {
		for {
			msg, err := ipfsPubSubSub.Next(ctx)
			if err != nil {
				_ = err // TODO
				break
			}

			if msg.From() == p.id {
				continue
			}

			msgTopic := msg.Topics()[0]

			if topic != msgTopic {
				continue
			}

			s.MessageChan() <- msg
		}
	}()

	go func() {
		<-ctx.Done()
		_ = s.Close()
		pm.Stop()
		delete(p.subscriptions, topic)
	}()

	return s, nil
}

func (p *pubSub) Publish(ctx context.Context, topic string, message []byte) error {
	if _, ok := p.subscriptions[topic]; !ok {
		return errors.New("to subscribed to this topic")
	}

	return p.ipfs.PubSub().Publish(ctx, topic, message)
}

func (p *pubSub) Close() error {
	for _, sub := range p.subscriptions {
		_ = sub.Close() // TODO: handle errors
	}

	return nil
}

func (p *pubSub) Unsubscribe(topic string) error {
	s, ok := p.subscriptions[topic]
	if !ok {
		return errors.New("no subscription found")
	}

	_ = s.Close()

	return nil
}

var _ Interface = &pubSub{}
