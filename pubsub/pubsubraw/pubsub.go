package pubsubraw

import (
	"context"
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
	p2ppubsub "github.com/libp2p/go-libp2p-pubsub"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/pubsub"
)

type psTopic struct {
	topic     *p2ppubsub.Topic
	ps        *rawPubSub
	topicName string
}

func (p *psTopic) Publish(ctx context.Context, message []byte) error {
	return p.topic.Publish(ctx, message)
}

func (p *psTopic) Peers(_ context.Context) ([]peer.ID, error) {
	return p.topic.ListPeers(), nil
}

func (p *psTopic) WatchPeers(ctx context.Context) (<-chan events.Event, error) {
	ph, err := p.topic.EventHandler()
	if err != nil {
		return nil, err
	}

	ch := make(chan events.Event, 32)
	go func() {
		defer close(ch)
		for {
			evt, err := ph.NextPeerEvent(ctx)
			if err != nil {
				switch err {
				case context.Canceled:
					p.ps.logger.Debug("watch peers ended", zap.Error(err))
				default:
					p.ps.logger.Error("watch next peer event failed", zap.Error(err))
				}
				return
			}

			switch evt.Type {
			case p2ppubsub.PeerJoin:
				ch <- pubsub.NewEventPeerJoin(evt.Peer)
			case p2ppubsub.PeerLeave:
				ch <- pubsub.NewEventPeerLeave(evt.Peer)
			}
		}
	}()

	return ch, nil
}

func (p *psTopic) WatchMessages(ctx context.Context) (<-chan *iface.EventPubSubMessage, error) {
	sub, err := p.topic.Subscribe(p2ppubsub.WithBufferSize(128))
	if err != nil {
		return nil, err
	}

	ch := make(chan *iface.EventPubSubMessage, 32)
	go func() {
		defer close(ch)
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				switch err {
				case context.Canceled, context.DeadlineExceeded:
					p.ps.logger.Debug("watch message ended",
						zap.String("topic", p.Topic()),
						zap.Error(err))
				default:
					p.ps.logger.Error("error while retrieving pubsub message",
						zap.String("topic", p.Topic()),
						zap.Error(err))
				}

				return
			}

			if msg.ReceivedFrom == p.ps.id {
				continue
			}

			ch <- pubsub.NewEventMessage(msg.Data)
		}
	}()

	return ch, nil
}

func (p *psTopic) Topic() string {
	return p.topicName
}

type rawPubSub struct {
	logger   *zap.Logger
	id       peer.ID
	tracer   trace.Tracer
	topics   map[string]*psTopic
	muTopics sync.Mutex
	pubsub   *p2ppubsub.PubSub
}

func (c *rawPubSub) TopicSubscribe(_ context.Context, topic string) (iface.PubSubTopic, error) {
	c.muTopics.Lock()
	defer c.muTopics.Unlock()

	if t, ok := c.topics[topic]; ok {
		return t, nil
	}

	joinedTopic, err := c.pubsub.Join(topic)
	if err != nil {
		return nil, err
	}

	c.topics[topic] = &psTopic{
		topicName: topic,
		topic:     joinedTopic,
		ps:        c,
	}

	return c.topics[topic], nil
}

func NewPubSub(ps *p2ppubsub.PubSub, id peer.ID, logger *zap.Logger, tracer trace.Tracer) iface.PubSubInterface {
	if logger == nil {
		logger = zap.NewNop()
	}

	if tracer == nil {
		tracer = trace.NewNoopTracerProvider().Tracer("")
	}

	return &rawPubSub{
		pubsub: ps,
		topics: map[string]*psTopic{},
		id:     id,
		logger: logger,
		tracer: tracer,
	}
}

var _ iface.PubSubInterface = &rawPubSub{}
var _ iface.PubSubTopic = &psTopic{}
