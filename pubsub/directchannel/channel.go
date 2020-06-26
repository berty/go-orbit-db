package directchannel

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"

	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/pubsub"
	p2pcore "github.com/libp2p/go-libp2p-core"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/protocol"
	"go.uber.org/zap"
)

const PROTOCOL = "ipfs-direct-channel/v1"

// Channel Channel is a pubsub used for a direct communication between peers
// new messages are received via events

type channel struct {
	events.EventEmitter
	receiverID p2pcore.PeerID
	logger     *zap.Logger
	host       host.Host
	stream     network.Stream
	muStream   sync.Mutex
}

func (c *channel) Send(ctx context.Context, bytes []byte) error {
	c.muStream.Lock()
	defer c.muStream.Unlock()
	if c.stream == nil {
		return fmt.Errorf("stream is not opened")
	}

	if len(bytes) > math.MaxUint16 {
		return fmt.Errorf("payload is too large")
	}

	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, uint16(len(bytes)))

	if _, err := c.stream.Write(b); err != nil {
		return err
	}

	_, err := c.stream.Write(bytes)
	return err
}

func (c *channel) Close() error {
	c.muStream.Lock()
	defer c.muStream.Unlock()

	c.stream = nil

	return nil
}

func (c *channel) incomingConnHandler(pid protocol.ID) (func(stream network.Stream), <-chan network.Stream) {
	once := sync.Once{}
	ch := make(chan network.Stream)

	return func(stream network.Stream) {
		once.Do(func() {
			c.host.RemoveStreamHandler(pid)

			ch <- stream
			close(ch)
		})
	}, ch
}

func (c *channel) Connect(ctx context.Context) error {
	var (
		err    error
		stream network.Stream
		id     protocol.ID
	)

	if strings.Compare(c.host.ID().String(), c.receiverID.String()) < 0 {
		id = protocol.ID(fmt.Sprintf("%s/%s", PROTOCOL, c.host.ID().String()))
		handler, doneCh := c.incomingConnHandler(id)

		c.host.SetStreamHandler(id, handler)
		select {
		case stream = <-doneCh:
			// nothing else to do, stream is acquired
		case <-ctx.Done():
			return fmt.Errorf("unable to create stream, err: %w", ctx.Err())
		}
	} else {
		id = protocol.ID(fmt.Sprintf("%s/%s", PROTOCOL, c.receiverID.String()))
		stream, err = c.host.NewStream(ctx, c.receiverID, id)
		if err != nil {
			return err
		}
	}

	c.muStream.Lock()
	c.stream = stream
	c.muStream.Unlock()

	go func() {
		for {
			b := make([]byte, 2)

			if _, err := stream.Read(b); err != nil {
				if err == io.EOF {
					return
				}

				c.logger.Error("unable to read", zap.Error(err))
				continue
			}

			data := make([]byte, binary.LittleEndian.Uint16(b))
			_, err := stream.Read(data)
			if err != nil {
				if err == io.EOF {
					return
				}

				c.logger.Error("unable to read", zap.Error(err))
				continue
			}

			c.Emit(ctx, pubsub.NewEventPayload(data))
		}
	}()

	return nil
}

func InitDirectChannelFactory(host host.Host) iface.DirectChannelFactory {
	return func(ctx context.Context, receiver p2pcore.PeerID, opts *iface.DirectChannelOptions) (iface.DirectChannel, error) {
		if opts == nil {
			opts = &iface.DirectChannelOptions{}
		}

		if opts.Logger == nil {
			opts.Logger = zap.NewNop()
		}

		return &channel{
			receiverID: receiver,
			logger:     opts.Logger,
			host:       host,
		}, nil
	}
}
