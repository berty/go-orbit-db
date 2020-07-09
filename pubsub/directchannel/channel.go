package directchannel

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	p2pcore "github.com/libp2p/go-libp2p-core"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/protocol"
	"go.uber.org/zap"

	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/pubsub"
)

const PROTOCOL = "ipfs-direct-channel/v1"

// Channel Channel is a pubsub used for a direct communication between peers
// new messages are received via events

type channel struct {
	events.EventEmitter
	receiverID p2pcore.PeerID
	logger     *zap.Logger
	holder     *channelHolder
	stream     io.Writer
	muStream   sync.Mutex
}

func (c *channel) Send(ctx context.Context, bytes []byte) error {
	c.muStream.Lock()
	stream := c.stream
	c.muStream.Unlock()

	if stream == nil {
		return fmt.Errorf("stream is not opened")
	}

	if len(bytes) > math.MaxUint16 {
		return fmt.Errorf("payload is too large")
	}

	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, uint16(len(bytes)))

	if _, err := stream.Write(b); err != nil {
		return err
	}

	_, err := stream.Write(bytes)
	return err
}

func (c *channel) Close() error {
	c.muStream.Lock()
	c.stream = nil
	c.muStream.Unlock()

	return nil
}

func (c *channel) Connect(ctx context.Context) error {
	var (
		err    error
		stream network.Stream
		id     protocol.ID
	)

	if strings.Compare(c.holder.host.ID().String(), c.receiverID.String()) < 0 {
		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		id = c.holder.hostProtocolID(c.receiverID)
		c.holder.muExpected.Lock()
		streamChan := c.holder.expectedPID[id]
		c.holder.muExpected.Unlock()

		select {
		case stream = <-streamChan:
			// nothing else to do, stream is acquired
		case <-ctx.Done():
			cancel()
			return fmt.Errorf("unable to create stream, err: %w", ctx.Err())
		}

		cancel()
	} else {
		id = c.holder.hostProtocolID(c.holder.host.ID())
		stream, err = c.holder.host.NewStream(ctx, c.receiverID, id)
		if err != nil {
			return err
		}
	}

	c.muStream.Lock()
	c.stream = stream
	c.muStream.Unlock()

	go func() {
		for {
			if err := c.incomingEvent(ctx, stream); err == io.EOF {
				return
			} else if err != nil {
				c.logger.Error("error while receiving event", zap.Error(err))
				return
			}
		}
	}()

	return nil
}

func (c *channel) incomingEvent(ctx context.Context, stream network.Stream) error {
	b := make([]byte, 2)

	if _, err := stream.Read(b); err != nil {
		if err == io.EOF {
			return err
		}

		c.logger.Error("unable to read", zap.Error(err))
		return err
	}

	data := make([]byte, binary.LittleEndian.Uint16(b))
	_, err := stream.Read(data)
	if err != nil {
		if err == io.EOF {
			return err
		}

		c.logger.Error("unable to read", zap.Error(err))
		return err
	}

	c.Emit(ctx, pubsub.NewEventPayload(data))

	return nil
}

type channelHolder struct {
	expectedPID map[protocol.ID]chan network.Stream
	host        host.Host
	muExpected  sync.Mutex
}

func (h *channelHolder) NewChannel(ctx context.Context, receiver p2pcore.PeerID, opts *iface.DirectChannelOptions) (iface.DirectChannel, error) {
	if opts == nil {
		opts = &iface.DirectChannelOptions{}
	}

	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}

	ch := &channel{
		receiverID: receiver,
		logger:     opts.Logger,
		holder:     h,
	}

	if strings.Compare(h.host.ID().String(), receiver.String()) < 0 {
		id := h.hostProtocolID(receiver)
		h.muExpected.Lock()
		h.expectedPID[id] = make(chan network.Stream)
		h.muExpected.Unlock()

		go func() {
			<-ctx.Done()
			h.muExpected.Lock()
			delete(h.expectedPID, id)
			h.muExpected.Unlock()
		}()
	}

	return ch, nil
}

func (h *channelHolder) hostProtocolID(receiver p2pcore.PeerID) protocol.ID {
	return protocol.ID(fmt.Sprintf("%s/%s", PROTOCOL, receiver.String()))
}

func (h *channelHolder) checkExpectedStream(s string) bool {
	h.muExpected.Lock()
	_, ok := h.expectedPID[protocol.ID(s)]
	h.muExpected.Unlock()

	return ok
}

func (h *channelHolder) incomingConnHandler(stream network.Stream) {
	h.muExpected.Lock()

	ch, ok := h.expectedPID[stream.Protocol()]
	if !ok {
		return
	}

	h.muExpected.Unlock()

	ch <- stream
}

func InitDirectChannelFactory(host host.Host) iface.DirectChannelFactory {
	holder := &channelHolder{
		expectedPID: map[protocol.ID]chan network.Stream{},
		host:        host,
	}

	host.SetStreamHandlerMatch(PROTOCOL, holder.checkExpectedStream, holder.incomingConnHandler)

	return holder.NewChannel
}
