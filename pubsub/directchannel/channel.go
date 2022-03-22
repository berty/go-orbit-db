package directchannel

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/pubsub"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.uber.org/zap"
)

const PROTOCOL = "/go-orbit-db/direct-channel/1.1.0"

type directChannel struct {
	logger  *zap.Logger
	host    host.Host
	emitter iface.DirectChannelEmitter
}

// Send Sends a message to the other peer
func (d *directChannel) Send(ctx context.Context, pid peer.ID, bytes []byte) error {
	stream, err := d.host.NewStream(ctx, pid, PROTOCOL)
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

	if err := d.emitter.Emit(pubsub.NewEventPayload(data, s.Conn().RemotePeer())); err != nil {
		d.logger.Error("unable to emit on emitter", zap.Error(err))
	}
}

// @NOTE(gfanton): we dont need this on direct channel
// Connect Waits for the other peer to be connected
func (d *directChannel) Connect(ctx context.Context, pid peer.ID) (err error) {
	return nil
}

// @NOTE(gfanton): we dont need this on direct channel
// Close Closes the connection
func (d *directChannel) Close() error {
	return d.emitter.Close()
}

type holderChannels struct {
	host   host.Host
	logger *zap.Logger
}

func (c *holderChannels) NewChannel(ctx context.Context, emitter iface.DirectChannelEmitter, opts *iface.DirectChannelOptions) (iface.DirectChannel, error) {
	if opts == nil {
		opts = &iface.DirectChannelOptions{}
	}

	if opts.Logger == nil {
		opts.Logger = c.logger
	}

	dc := &directChannel{
		logger:  c.logger,
		host:    c.host,
		emitter: emitter,
	}

	c.host.SetStreamHandler(PROTOCOL, dc.handleNewPeer)
	return dc, nil

}

func InitDirectChannelFactory(logger *zap.Logger, host host.Host) iface.DirectChannelFactory {
	holder := &holderChannels{
		logger: logger,
		host:   host,
	}

	return holder.NewChannel
}
