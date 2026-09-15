package pubsubcoreapi

import (
	"context"
	"sync"
	"time"

	coreiface "github.com/ipfs/kubo/core/coreiface"
	"github.com/ipfs/kubo/core/coreiface/options"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"

	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/pubsub"
)

// DefaultRecoverGracePolls is how many consecutive WatchPeers polls a peer must
// be observed as libp2p-connected-but-absent from the topic before it is
// reconciled back in. It guards against the normal connect/subscribe window
// (where a freshly connected peer briefly isn't in the topic yet) while still
// recovering from the go-libp2p-pubsub connect/disconnect race quickly.
const DefaultRecoverGracePolls = 3

type psTopic struct {
	topic     string
	ps        *coreAPIPubSub
	members   []peer.ID
	muMembers sync.RWMutex

	// knownMembers are peers that have been seen subscribed to this topic at
	// least once. Only these are eligible for connectedness reconciliation, so
	// we never resurrect peers that were never part of the topic.
	knownMembers map[peer.ID]struct{}
	// absentCounts tracks, per known-and-still-connected peer, how many
	// consecutive polls it has been missing from the topic.
	absentCounts map[peer.ID]int
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

	topicMembers, err := p.ps.topicPeers(ctx, p.topic)
	if err != nil {
		return nil, nil, err
	}

	all := p.reconcile(ctx, topicMembers)

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

// reconcile augments the set of pubsub topic members with peers that are still
// connected at the libp2p level but have silently fallen out of the topic.
//
// This works around a go-libp2p-pubsub connect/disconnect race: when a peer
// disconnects and reconnects quickly, pubsub can process the new-connection
// event before the matching disconnect event, dedupe the reconnecting peer
// against its own stale entry, and never re-propagate its subscription. The
// libp2p connection is healthy but the peer ends up listed in zero topics, so
// no PeerJoin ever fires and the store never re-triggers exchangeHeads — the
// reconnected peer can never catch up.
//
// We detect such peers (previously seen in this topic, still connected, absent
// for more than DefaultRecoverGracePolls polls) and treat them as members
// again. That re-emits a PeerJoin, which resumes head exchange over the direct
// channel — independent of whether pubsub itself ever heals the subscription.
func (p *psTopic) reconcile(ctx context.Context, topicMembers []peer.ID) []peer.ID {
	inTopic := make(map[peer.ID]struct{}, len(topicMembers))
	for _, m := range topicMembers {
		inTopic[m] = struct{}{}
		p.knownMembers[m] = struct{}{}
		delete(p.absentCounts, m)
	}

	// recoverGracePolls <= 0 disables reconciliation and preserves the legacy
	// behaviour of trusting the pubsub topic membership verbatim.
	if p.ps.recoverGracePolls <= 0 {
		return topicMembers
	}

	connected := p.ps.connectedPeers(ctx)
	connectedSet := make(map[peer.ID]struct{}, len(connected))

	effective := append([]peer.ID(nil), topicMembers...)
	for _, pid := range connected {
		connectedSet[pid] = struct{}{}

		if _, ok := inTopic[pid]; ok {
			continue
		}

		if _, known := p.knownMembers[pid]; !known {
			// never part of this topic: not a lost subscription, ignore it.
			continue
		}

		p.absentCounts[pid]++
		if p.absentCounts[pid] >= p.ps.recoverGracePolls {
			effective = append(effective, pid)
		}
	}

	// Forget absence bookkeeping for peers that are no longer connected; a real
	// PeerLeave is emitted for them through the normal diff.
	for pid := range p.absentCounts {
		if _, ok := connectedSet[pid]; !ok {
			delete(p.absentCounts, pid)
		}
	}

	return effective
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
	api          coreiface.CoreAPI
	logger       *zap.Logger
	id           peer.ID
	pollInterval time.Duration
	tracer       trace.Tracer
	topics       map[string]*psTopic
	muTopics     sync.Mutex

	// recoverGracePolls drives the connectedness-based reconciliation in
	// psTopic.reconcile (see DefaultRecoverGracePolls). 0 disables it.
	recoverGracePolls int

	// topicPeers returns the peers pubsub currently lists for a topic, and
	// connectedPeers returns the peers we are connected to at the libp2p level.
	// They are fields so tests can drive the reconnect race deterministically
	// without a real IPFS node; in production they wrap the CoreAPI.
	topicPeers     func(ctx context.Context, topic string) ([]peer.ID, error)
	connectedPeers func(ctx context.Context) []peer.ID
}

func (c *coreAPIPubSub) defaultTopicPeers(ctx context.Context, topic string) ([]peer.ID, error) {
	return c.api.PubSub().Peers(ctx, options.PubSub.Topic(topic))
}

func (c *coreAPIPubSub) defaultConnectedPeers(ctx context.Context) []peer.ID {
	conns, err := c.api.Swarm().Peers(ctx)
	if err != nil {
		c.logger.Debug("unable to list swarm peers for reconciliation", zap.Error(err))
		return nil
	}

	ids := make([]peer.ID, len(conns))
	for i, conn := range conns {
		ids[i] = conn.ID()
	}

	return ids
}

func (c *coreAPIPubSub) TopicSubscribe(_ context.Context, topic string) (iface.PubSubTopic, error) {
	c.muTopics.Lock()
	defer c.muTopics.Unlock()

	if t, ok := c.topics[topic]; ok {
		return t, nil
	}

	c.topics[topic] = &psTopic{
		topic:        topic,
		ps:           c,
		knownMembers: map[peer.ID]struct{}{},
		absentCounts: map[peer.ID]int{},
	}

	return c.topics[topic], nil
}

func NewPubSub(api coreiface.CoreAPI, id peer.ID, pollInterval time.Duration, logger *zap.Logger, tracer trace.Tracer) iface.PubSubInterface {
	if logger == nil {
		logger = zap.NewNop()
	}

	if tracer == nil {
		tracer = tracenoop.NewTracerProvider().Tracer("")
	}

	c := &coreAPIPubSub{
		topics:            map[string]*psTopic{},
		api:               api,
		id:                id,
		logger:            logger,
		pollInterval:      pollInterval,
		tracer:            tracer,
		recoverGracePolls: DefaultRecoverGracePolls,
	}

	c.topicPeers = c.defaultTopicPeers
	c.connectedPeers = c.defaultConnectedPeers

	return c
}

var _ iface.PubSubInterface = &coreAPIPubSub{}
var _ iface.PubSubTopic = &psTopic{}
