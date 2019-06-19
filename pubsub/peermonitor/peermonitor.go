package peermonitor

import (
	"context"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
	"github.com/libp2p/go-libp2p-core/peer"
	"time"
)

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
	cancelFunc     func()
	pubsub         coreapi.PubSubAPI
	topic          string
	started        bool
	pollInterval   time.Duration
	peers          map[peer.ID]struct{}
	peerChangeChan chan *Event
}

func (p *peerMonitor) Start(ctx context.Context) func() {
	if p.started == true {
		p.Stop()
	}

	ctx, cancelFunc := context.WithCancel(ctx)

	p.started = true
	p.cancelFunc = cancelFunc

	go func() {
		for {
			select {
			case <-ctx.Done():
				p.cancelFunc = nil
				p.started = false
				return

			case <-time.After(p.pollInterval):
				err := p.pollPeers(ctx)
				_ = err // TODO: handle error

				break
			}
		}
	}()

	return cancelFunc
}

func (p *peerMonitor) Stop() {
	if p.cancelFunc == nil {
		return
	}

	p.cancelFunc()
}

func (p *peerMonitor) GetPeers() []peer.ID {
	var peerIDs []peer.ID
	for p := range p.peers {
		peerIDs = append(peerIDs, p)
	}

	return peerIDs
}

func (p *peerMonitor) HasPeer(id peer.ID) bool {
	_, ok := p.peers[id]

	return ok
}

func (p *peerMonitor) pollPeers(ctx context.Context) error {
	peerIDs, err := p.pubsub.Peers(ctx)

	currentPeers := map[peer.ID]struct{}{}
	allPeers := map[peer.ID]struct{}{}
	newPeers := map[peer.ID]struct{}{}

	for peerID := range p.peers {
		currentPeers[peerID] = struct{}{}
	}

	if err != nil {
		return err
	}

	for _, peerID := range peerIDs {
		allPeers[peerID] = struct{}{}

		if _, ok := currentPeers[peerID]; ok {
			delete(currentPeers, peerID)
		} else if _, ok := allPeers[peerID]; !ok {
			newPeers[peerID] = struct{}{}
		}
	}

	for peerID := range currentPeers {
		p.peerChangeChan <- &Event{Action: EventActionLeave, Peer: peerID}
	}

	for peerID := range newPeers {
		p.peerChangeChan <- &Event{Action: EventActionJoin, Peer: peerID}
	}

	p.peers = allPeers

	return nil
}

func NewPeerMonitor(ctx context.Context, pubsub coreapi.PubSubAPI, topic string, peerChangeChan chan *Event, options *NewPeerMonitorOptions) Interface {
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
		pubsub:         pubsub,
		topic:          topic,
		pollInterval:   *options.PollInterval,
		peerChangeChan: peerChangeChan,
	}

	if *options.Start == true {
		monitor.Start(ctx)
	}

	return monitor
}

var _ Interface = &peerMonitor{}
