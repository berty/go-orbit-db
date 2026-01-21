package directchannel

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/stateless-minds/go-orbit-db/iface"
	"github.com/stateless-minds/go-orbit-db/pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
)

const PROTOCOL = "/go-orbit-db/direct-channel/1.2.0"
const DelimitedReadMaxSize = 1024 * 1024 * 4 // mb

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

	defer stream.Close()

	length := uint64(len(bytes))
	lenbuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(lenbuf, length)
	_, err = stream.Write(lenbuf[:n])
	if err != nil {
		return fmt.Errorf("unable to write buflen: %w", err)
	}
	_, err = stream.Write(bytes)
	return err
}

func (d *directChannel) handleNewPeer(s network.Stream) {
	defer func() {
		_ = s.Reset()
	}()

	reader := bufio.NewReader(s)
	length64, err := binary.ReadUvarint(reader)
	if err != nil {
		d.logger.Error("unable to read length", zap.Error(err))
		return
	}

	length := int(length64)

	if length > DelimitedReadMaxSize {
		d.logger.Error(fmt.Sprintf("received data exceeding maximum allowed size (%d > %d)", length, DelimitedReadMaxSize))
		return
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(reader, data); err != nil {
		d.logger.Error("unable to read buffer", zap.Error(err))
		return
	}

	if err := d.emitter.Emit(pubsub.NewEventPayload(data, s.Conn().RemotePeer())); err != nil {
		d.logger.Error("unable to emit on emitter", zap.Error(err))
	}
}

// @NOTE(gfanton): we dont need this on direct channel
// Connect Waits for the other peer to be connected
func (d *directChannel) Connect(ctx context.Context, pid peer.ID) (err error) { //nolint:all
	return nil
}

// @NOTE(gfanton): we dont need this on direct channel
// Close Closes the connection
func (d *directChannel) Close() error {
	d.host.RemoveStreamHandler(PROTOCOL)
	return d.emitter.Close()
}

type holderChannels struct {
	host   host.Host
	logger *zap.Logger
}

func (c *holderChannels) NewChannel(_ context.Context, emitter iface.DirectChannelEmitter, opts *iface.DirectChannelOptions) (iface.DirectChannel, error) {
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
