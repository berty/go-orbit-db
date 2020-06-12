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
	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/trace"
	"go.uber.org/zap"
)

type subscription struct {
	events.EventEmitter
	pubSubSub iface.PubSubSubscription
	ipfs      coreapi.CoreAPI
	id        p2pcore.PeerID
	logger    *zap.Logger
	span      trace.Span
}

type Options struct {
	Logger *zap.Logger
	Tracer trace.Tracer
}

// NewSubscription Creates a new pub sub subscription
func NewSubscription(ctx context.Context, ipfs coreapi.CoreAPI, topic string, opts *Options) (Subscription, error) {
	if opts == nil {
		opts = &Options{}
	}

	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}

	if opts.Tracer == nil {
		opts.Tracer = trace.NoopTracer{}
	}

	ctx, span := opts.Tracer.Start(ctx, "pubsub-subscription", trace.WithAttributes(kv.String("pubsub-topic", topic)))
	go func() {
		<-ctx.Done()
		span.End()
	}()

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
		logger:    opts.Logger,
		span:      span,
	}

	go s.listener(ctx, pubSubSub, topic)
	go s.topicMonitor(ctx, topic)

	return s, nil
}

func (s *subscription) Close() error {
	if err := s.pubSubSub.Close(); err != nil {
		s.logger.Error("error while closing subscription", zap.Error(err))
	}

	return nil
}

func (s *subscription) topicMonitor(ctx context.Context, topic string) {
	pm := peermonitor.NewPeerMonitor(ctx, s.ipfs, topic, &peermonitor.NewPeerMonitorOptions{Logger: s.logger})

	go func() {
		for evt := range pm.Subscribe(ctx) {
			switch e := evt.(type) {
			case *peermonitor.EventPeerJoin:
				s.span.AddEvent(ctx, "pubsub-join", kv.String("topic", topic), kv.String("peerid", e.Peer.String()))
				s.logger.Debug(fmt.Sprintf("peer %s joined topic %s", e.Peer, topic))

			case *peermonitor.EventPeerLeave:
				s.span.AddEvent(ctx, "pubsub-leave", kv.String("topic", topic), kv.String("peerid", e.Peer.String()))
				s.logger.Debug(fmt.Sprintf("peer %s left topic %s", e.Peer, topic))
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
				s.logger.Error("unable to get pub sub message", zap.Error(err))
			}

			break
		}

		if msg.From() == s.id {
			continue
		}

		msgTopic := msg.Topics()[0]

		if topic != msgTopic {
			s.logger.Debug("message is from another topic, ignoring")
			continue
		}

		s.logger.Debug(fmt.Sprintf("got pub sub message from %s", s.id))

		s.span.AddEvent(ctx, "pubsub-new-message", kv.String("topic", topic), kv.String("peerid", msg.From().String()))
		s.Emit(ctx, NewMessageEvent(topic, msg.Data()))
	}
}

var _ Subscription = &subscription{}
