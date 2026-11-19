package pubsubcoreapi

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"

	"berty.tech/go-orbit-db/iface"
)

// Phases of the go-libp2p-pubsub connect/disconnect race, as observed from the
// receiver's perspective on a shared topic:
//
//   - phaseMember : the peer is a healthy topic member (connected + in topic).
//   - phaseOffline: the peer disconnects (gone from both the topic and libp2p).
//   - phaseStuck  : the peer reconnects *at the libp2p level* but, because of
//     the pubsub connect/disconnect race, its subscription is never
//     re-propagated. The libp2p connection is healthy yet the peer is listed in
//     zero topics. No PeerJoin ever fires, so the store never re-triggers
//     exchangeHeads and the reconnected peer can never catch up.
const (
	phaseMember = iota
	phaseOffline
	phaseStuck
)

// newPhasedPubSub builds a coreAPIPubSub whose view of the world is driven by a
// single atomic "phase" instead of a real IPFS node, so the race is fully
// deterministic. topicPeers models pubsub topic membership; connectedPeers
// models libp2p connectedness. In phaseStuck the peer is connected but missing
// from the topic, exactly reproducing the lost-subscription bug.
func newPhasedPubSub(grace int, peerA peer.ID, phase *atomic.Int32) *coreAPIPubSub {
	c := &coreAPIPubSub{
		topics:            map[string]*psTopic{},
		id:                peer.ID("self"),
		pollInterval:      2 * time.Millisecond,
		logger:            zap.NewNop(),
		tracer:            tracenoop.NewTracerProvider().Tracer(""),
		recoverGracePolls: grace,
	}

	c.topicPeers = func(_ context.Context, _ string) ([]peer.ID, error) {
		if phase.Load() == phaseMember {
			return []peer.ID{peerA}, nil
		}
		// Offline: genuinely gone. Stuck: silently dropped from the topic
		// while the libp2p connection stays up (the bug).
		return nil, nil
	}

	c.connectedPeers = func(_ context.Context) []peer.ID {
		switch phase.Load() {
		case phaseMember, phaseStuck:
			return []peer.ID{peerA}
		default: // phaseOffline
			return nil
		}
	}

	return c
}

// expectJoin waits for a PeerJoin for peerA within timeout.
func expectJoin(t *testing.T, ch <-chan eventsEvent, peerA peer.ID, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case e := <-ch:
			if j, ok := e.(*iface.EventPubSubJoin); ok && j.Peer == peerA {
				return
			}
			// ignore other events (e.g. a leave) and keep waiting
		case <-deadline:
			t.Fatalf("timed out waiting for PeerJoin of %s", peerA)
		}
	}
}

// expectLeave waits for a PeerLeave for peerA within timeout.
func expectLeave(t *testing.T, ch <-chan eventsEvent, peerA peer.ID, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case e := <-ch:
			if l, ok := e.(*iface.EventPubSubLeave); ok && l.Peer == peerA {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for PeerLeave of %s", peerA)
		}
	}
}

// expectNoJoin asserts that no PeerJoin for peerA arrives within the window.
func expectNoJoin(t *testing.T, ch <-chan eventsEvent, peerA peer.ID, window time.Duration) {
	t.Helper()
	deadline := time.After(window)
	for {
		select {
		case e := <-ch:
			if j, ok := e.(*iface.EventPubSubJoin); ok && j.Peer == peerA {
				t.Fatalf("unexpected PeerJoin of %s while reconciliation disabled", peerA)
			}
		case <-deadline:
			return
		}
	}
}

// eventsEvent is the element type emitted on the WatchPeers channel.
type eventsEvent = interface{}

// runPhases drives a single psTopic through member -> offline -> stuck and
// returns the channel so the caller can assert on the phaseStuck behaviour.
func runPhases(t *testing.T, ps *coreAPIPubSub, peerA peer.ID, phase *atomic.Int32) <-chan eventsEvent {
	t.Helper()

	topic, err := ps.TopicSubscribe(context.Background(), "store-addr")
	if err != nil {
		t.Fatal(err)
	}

	raw, err := topic.WatchPeers(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// adapt the typed channel to interface{}
	ch := make(chan eventsEvent, 32)
	go func() {
		for e := range raw {
			ch <- e
		}
		close(ch)
	}()

	// phaseMember: the peer is a healthy member -> a join is emitted.
	expectJoin(t, ch, peerA, time.Second)

	// phaseOffline: the peer disconnects -> a leave is emitted.
	phase.Store(phaseOffline)
	expectLeave(t, ch, peerA, time.Second)

	// phaseStuck: the peer reconnects at the libp2p level but its subscription
	// is silently lost (the race). From now on it is connected-but-not-in-topic.
	phase.Store(phaseStuck)

	return ch
}

// TestReconnectRaceReplicationRecovery reproduces the go-orbit-db replication
// stall caused by the go-libp2p-pubsub connect/disconnect race, and asserts
// that the connectedness-based reconciliation recovers from it.
//
// The two subtests share the exact same scenario; the only difference is
// whether reconciliation is enabled (recoverGracePolls > 0):
//
//   - "issue": with reconciliation disabled (the pre-fix behaviour), the stuck
//     peer never rejoins the topic, so no PeerJoin fires and the store would
//     never re-trigger exchangeHeads -> the reconnected peer is stuck forever.
//   - "fix": with reconciliation enabled, the still-connected peer is detected
//     and re-emitted as a member, so a PeerJoin fires and head exchange resumes.
func TestReconnectRaceReplicationRecovery(t *testing.T) {
	peerA := peer.ID("peer-A")

	t.Run("issue: stuck peer never recovers without reconciliation", func(t *testing.T) {
		var phase atomic.Int32
		phase.Store(phaseMember)

		ps := newPhasedPubSub(0 /* reconciliation disabled */, peerA, &phase)
		ch := runPhases(t, ps, peerA, &phase)

		// The peer is connected but absent from the topic. Pre-fix, nothing
		// ever re-triggers it: assert no PeerJoin arrives. We wait a full
		// second (as in expectJoin) to be confident the peer truly never
		// rejoins, rather than just not yet.
		expectNoJoin(t, ch, peerA, time.Second)
	})

	t.Run("fix: stuck peer recovers via connectedness reconciliation", func(t *testing.T) {
		var phase atomic.Int32
		phase.Store(phaseMember)

		ps := newPhasedPubSub(3 /* reconciliation enabled */, peerA, &phase)
		ch := runPhases(t, ps, peerA, &phase)

		// The still-connected peer is reconciled back into the topic, so a
		// fresh PeerJoin fires and exchangeHeads can resume.
		expectJoin(t, ch, peerA, time.Second)
	})
}
