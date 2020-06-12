package pubsub

import (
	"context"
	"fmt"
	"sync"

	coreapi "github.com/ipfs/interface-go-ipfs-core"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/trace"
	"go.uber.org/zap"
)

type pubSub struct {
	coreAPI         coreapi.CoreAPI
	id              peer.ID
	subscriptions   map[string]Subscription
	muSubscriptions sync.RWMutex
	logger          *zap.Logger
	tracer          trace.Tracer
}

// NewPubSub Creates a new pubsub client
func NewPubSub(coreAPI coreapi.CoreAPI, id peer.ID, opts *Options) (Interface, error) {
	if opts == nil {
		opts = &Options{}
	}

	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}

	if opts.Tracer == nil {
		opts.Tracer = trace.NoopTracer{}
	}

	if coreAPI == nil {
		return nil, errors.New("coreAPI is not defined")
	}

	if ps := coreAPI.PubSub(); ps == nil {
		return nil, errors.New("pubsub service is not provided by the current ipfs instance")
	}

	return &pubSub{
		coreAPI:       coreAPI,
		id:            id,
		subscriptions: map[string]Subscription{},
		logger:        opts.Logger,
		tracer:        opts.Tracer,
	}, nil
}

func (p *pubSub) Subscribe(ctx context.Context, topic string) (Subscription, error) {
	p.muSubscriptions.Lock()
	defer p.muSubscriptions.Unlock()

	if sub, ok := p.subscriptions[topic]; ok {
		return sub, nil
	}

	p.logger.Debug(fmt.Sprintf("starting pubsub listener for peer %s on topic %s", p.id, topic))

	s, err := NewSubscription(ctx, p.coreAPI, topic, &Options{
		Logger: p.logger,
		Tracer: p.tracer,
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new pubsub subscription")
	}

	p.subscriptions[topic] = s

	return s, nil
}

func (p *pubSub) Publish(ctx context.Context, topic string, message []byte) error {
	p.muSubscriptions.RLock()
	if _, ok := p.subscriptions[topic]; !ok {
		return errors.New("not subscribed to this topic")
	}
	p.muSubscriptions.RUnlock()

	ctx, span := p.tracer.Start(ctx, "pubsub-publish", trace.WithAttributes(kv.String("topic", topic), kv.String("peerid", p.id.String())))
	defer span.End()

	return p.coreAPI.PubSub().Publish(ctx, topic, message)
}

func (p *pubSub) Close() error {
	p.muSubscriptions.RLock()
	subs := p.subscriptions
	p.muSubscriptions.RUnlock()

	for _, sub := range subs {
		_ = sub.Close()
	}

	return nil
}

func (p *pubSub) Unsubscribe(topic string) error {
	p.muSubscriptions.RLock()
	s, ok := p.subscriptions[topic]
	p.muSubscriptions.RUnlock()

	if !ok {
		return errors.New("no subscription found")
	}

	_ = s.Close()

	return nil
}

var _ Interface = &pubSub{}
