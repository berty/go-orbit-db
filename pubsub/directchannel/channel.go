package directchannel

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"

	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/pubsub"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.uber.org/zap"
)

const PROTOCOL = "/go-orbit-db/direct-channel/1.1.0"

type directChannel struct {
	events.EventEmitter

	logger *zap.Logger
	host   host.Host
	peer   peer.ID
}

// Send Sends a message to the other peer
func (d *directChannel) Send(ctx context.Context, bytes []byte) error {
	stream, err := d.host.NewStream(ctx, d.peer, PROTOCOL)
	if err != nil {
		return fmt.Errorf("unable to create stream: %w", err)
	}

	if len(bytes) > math.MaxUint16 {
		return fmt.Errorf("payload is too large")
	}

	if len(bytes) > math.MaxUint16 {
		return fmt.Errorf("payload is too large")
	}

	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, uint16(len(bytes)))

	if _, err := stream.Write(b); err != nil {
		return err
	}

	_, err = stream.Write(bytes)
	return err
}

func (d *directChannel) handleNewPeer(s network.Stream) {
	b := make([]byte, 2)
	defer func() {
		_ = s.Reset()
	}()

	if _, err := s.Read(b); err != nil {
		if err == io.EOF {
			return
		}

		d.logger.Error("unable to read", zap.Error(err))
		return
	}

	data := make([]byte, binary.LittleEndian.Uint16(b))
	_, err := s.Read(data)
	if err != nil {
		if err == io.EOF {
			return
		}

		d.logger.Error("unable to read", zap.Error(err))
		return
	}

	d.Emit(context.Background(), pubsub.NewEventPayload(data))
}

// @NOTE(gfanton): we dont need this on direct channel
// Connect Waits for the other peer to be connected
func (d *directChannel) Connect(ctx context.Context) (err error) {
	return nil
}

// @NOTE(gfanton): we dont need this on direct channel
// Close Closes the connection
func (d *directChannel) Close() error {
	return nil
}

type holderChannels struct {
	muChannels sync.Mutex
	channels   map[peer.ID]*directChannel
	host       host.Host
	logger     *zap.Logger
}

func (c *holderChannels) incomingConnHandler(s network.Stream) {
	remotepeer := s.Conn().RemotePeer()

	c.muChannels.Lock()
	dc, ok := c.channels[remotepeer]
	if !ok {
		dc = &directChannel{
			logger: c.logger,
			host:   c.host,
			peer:   remotepeer,
		}
		c.channels[remotepeer] = dc
	}
	c.muChannels.Unlock()

	go dc.handleNewPeer(s)
}

func (c *holderChannels) NewChannel(ctx context.Context, receiver peer.ID, opts *iface.DirectChannelOptions) (iface.DirectChannel, error) {
	if opts == nil {
		opts = &iface.DirectChannelOptions{}
	}

	if opts.Logger == nil {
		opts.Logger = c.logger
	}

	c.muChannels.Lock()
	defer c.muChannels.Unlock()

	if channel, ok := c.channels[receiver]; ok {
		return channel, nil
	}

	dc := &directChannel{
		logger: c.logger,
		host:   c.host,
		peer:   receiver,
	}
	c.channels[receiver] = dc
	return dc, nil
}

func InitDirectChannelFactory(logger *zap.Logger, host host.Host) iface.DirectChannelFactory {
	holder := &holderChannels{
		logger:   logger,
		channels: make(map[peer.ID]*directChannel),
		host:     host,
	}

	host.SetStreamHandler(PROTOCOL, holder.incomingConnHandler)

	return holder.NewChannel
}
