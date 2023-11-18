package pubsubcoreapi

import (
	"context"
	"sync"
	"time"

	coreapi "github.com/ipfs/boxo/coreiface"
	options "github.com/ipfs/boxo/coreiface/options"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/pubsub"
)

type psTopic struct {
	topic     string
	ps        *coreAPIPubSub
	members   []peer.ID
	muMembers sync.RWMutex
}

func (p *psTopic) Publish(ctx context.Context, message []byte) error {
	return p.ps.api.PubSub().Publish(ctx, p.topic, message)
}

func (p *psTopic) Peers(_ context.Context) ([]peer.ID, error) {
	p.muMembers.RLock()
	members := p.members
	p.muMembers.RUnlock()

	return members, nil
}

func (p *psTopic) peersDiff(ctx context.Context) (joining, leaving []peer.ID, err error) {
	p.muMembers.RLock()
	oldMembers := map[peer.ID]struct{}{}

	for _, m := range p.members {
		oldMembers[m] = struct{}{}
	}
	p.muMembers.RUnlock()

	all, err := p.ps.api.PubSub().Peers(ctx, options.PubSub.Topic(p.topic))
	if err != nil {
		return nil, nil, err
	}

	for _, m := range all {
		if _, ok := oldMembers[m]; !ok {
			joining = append(joining, m)
		} else {
			delete(oldMembers, m)
		}
	}

	for m := range oldMembers {
		leaving = append(leaving, m)
	}

	p.muMembers.Lock()
	p.members = all
	p.muMembers.Unlock()

	return joining, leaving, nil
}

func (p *psTopic) WatchPeers(ctx context.Context) (<-chan events.Event, error) {
	ch := make(chan events.Event, 32)
	go func() {
		defer close(ch)
		for {
			joining, leaving, err := p.peersDiff(ctx)
			if err != nil {
				p.ps.logger.Error("", zap.Error(err))
				return
			}

			for _, pid := range joining {
				ch <- pubsub.NewEventPeerJoin(pid, p.Topic())
			}

			for _, pid := range leaving {
				ch <- pubsub.NewEventPeerLeave(pid, p.Topic())
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(p.ps.pollInterval):
				continue
			}
		}
	}()

	return ch, nil
}

func (p *psTopic) WatchMessages(ctx context.Context) (<-chan *iface.EventPubSubMessage, error) {
	sub, err := p.ps.api.PubSub().Subscribe(ctx, p.topic)
	if err != nil {
		return nil, err
	}

	ch := make(chan *iface.EventPubSubMessage, 128)
	go func() {
		defer close(ch)
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				switch err {
				case context.Canceled, context.DeadlineExceeded:
					p.ps.logger.Debug("watch message ended",
						zap.String("topic", p.topic),
						zap.Error(err))
				default:
					p.ps.logger.Error("error while retrieving pubsub message",
						zap.String("topic", p.topic),
						zap.Error(err))
				}

				return
			}

			if msg.From() == p.ps.id {
				continue
			}

			ch <- pubsub.NewEventMessage(msg.Data())
		}
	}()

	return ch, nil
}

func (p *psTopic) Topic() string {
	return p.topic
}

type coreAPIPubSub struct {
	api          coreapi.CoreAPI
	logger       *zap.Logger
	id           peer.ID
	pollInterval time.Duration
	tracer       trace.Tracer
	topics       map[string]*psTopic
	muTopics     sync.Mutex
}

func (c *coreAPIPubSub) TopicSubscribe(_ context.Context, topic string) (iface.PubSubTopic, error) {
	c.muTopics.Lock()
	defer c.muTopics.Unlock()

	if t, ok := c.topics[topic]; ok {
		return t, nil
	}

	c.topics[topic] = &psTopic{
		topic: topic,
		ps:    c,
	}

	return c.topics[topic], nil
}

func NewPubSub(api coreapi.CoreAPI, id peer.ID, pollInterval time.Duration, logger *zap.Logger, tracer trace.Tracer) iface.PubSubInterface {
	if logger == nil {
		logger = zap.NewNop()
	}

	if tracer == nil {
		tracer = trace.NewNoopTracerProvider().Tracer("")
	}

	return &coreAPIPubSub{
		topics:       map[string]*psTopic{},
		api:          api,
		id:           id,
		logger:       logger,
		pollInterval: pollInterval,
		tracer:       tracer,
	}
}

var _ iface.PubSubInterface = &coreAPIPubSub{}
var _ iface.PubSubTopic = &psTopic{}
