package peermonitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"berty.tech/go-orbit-db/events"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.uber.org/zap"
)

// NewPeerMonitorOptions Options for creating a new PeerMonitor instance
type NewPeerMonitorOptions struct {
	Start        *bool
	PollInterval *time.Duration
}

func durationPtr(duration time.Duration) *time.Duration {
	return &duration
}

func boolPtr(val bool) *bool {
	return &val
}

var defaultPeerMonitorOptions = &NewPeerMonitorOptions{
	Start:        boolPtr(true),
	PollInterval: durationPtr(time.Second),
}

type peerMonitor struct {
	events.EventEmitter
	cancelFunc   func()
	ipfs         coreapi.CoreAPI
	topic        string
	started      bool
	pollInterval time.Duration
	peers        map[peer.ID]struct{}

	muPeers   sync.RWMutex
	muStarted sync.RWMutex
}

func (p *peerMonitor) Start(ctx context.Context) func() {
	if p.Started() {
		p.Stop()
	}

	ctx, cancelFunc := context.WithCancel(ctx)

	p.muStarted.Lock()
	p.started = true
	p.cancelFunc = cancelFunc
	p.muStarted.Unlock()

	go func() {
		for {
			select {
			case <-ctx.Done():
				p.muStarted.Lock()
				p.cancelFunc = nil
				p.started = false
				p.muStarted.Unlock()
				return

			case <-time.After(p.pollInterval):
				err := p.pollPeers(ctx)
				if err != nil {
					logger().Error("error while polling peers", zap.Error(err))
				}

				break
			}
		}
	}()

	return cancelFunc
}

func (p *peerMonitor) Stop() {
	p.muStarted.RLock()
	cancelFunc := p.cancelFunc
	p.muStarted.RUnlock()

	if cancelFunc != nil {
		cancelFunc()
	}
}

func (p *peerMonitor) GetPeers() []peer.ID {
	p.muPeers.RLock()
	defer p.muPeers.RUnlock()

	peerIDs := make([]peer.ID, len(p.peers))
	i := 0

	for p := range p.peers {
		peerIDs[i] = p
		i++
	}

	return peerIDs
}

func (p *peerMonitor) HasPeer(id peer.ID) bool {
	p.muPeers.RLock()
	defer p.muPeers.RUnlock()

	_, ok := p.peers[id]

	return ok
}

func (p *peerMonitor) pollPeers(ctx context.Context) error {
	p.muPeers.Lock()
	defer p.muPeers.Unlock()

	peerIDs, err := p.ipfs.PubSub().Peers(ctx, options.PubSub.Topic(p.topic))
	logger().Debug(fmt.Sprintf("polling peers for topic %s", p.topic))

	currentlyKnownPeers := map[peer.ID]struct{}{}
	allPeers := map[peer.ID]struct{}{}

	for peerID := range p.peers {
		currentlyKnownPeers[peerID] = struct{}{}
	}

	if err != nil {
		return err
	}

	for _, peerID := range peerIDs {
		allPeers[peerID] = struct{}{}

		if _, ok := currentlyKnownPeers[peerID]; ok {
			delete(currentlyKnownPeers, peerID)
		} else {
			p.Emit(NewEventPeerJoin(peerID))
		}
	}

	for peerID := range currentlyKnownPeers {
		p.Emit(NewEventPeerLeave(peerID))
	}

	p.peers = allPeers

	return nil
}

func (p *peerMonitor) Started() bool {
	p.muStarted.RLock()
	defer p.muStarted.RUnlock()

	return p.started
}

// NewPeerMonitor Creates a new PeerMonitor instance
func NewPeerMonitor(ctx context.Context, ipfs coreapi.CoreAPI, topic string, options *NewPeerMonitorOptions) Interface {
	if options == nil {
		options = defaultPeerMonitorOptions
	}

	if options.PollInterval == nil {
		options.PollInterval = defaultPeerMonitorOptions.PollInterval
	}

	if options.Start == nil {
		options.Start = defaultPeerMonitorOptions.Start
	}

	monitor := &peerMonitor{
		ipfs:         ipfs,
		topic:        topic,
		pollInterval: *options.PollInterval,
	}

	if *options.Start == true {
		monitor.Start(ctx)
	}

	return monitor
}

var _ Interface = &peerMonitor{}
