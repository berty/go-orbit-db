package directchannel

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/pubsub"
	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestInitDirectChannelFactory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mn := mocknet.New(ctx)

	var err error
	count := 10
	hosts := make([]host.Host, count)
	eventBus := make([]event.Bus, count)
	dc := make([]iface.DirectChannel, count)

	for i := 0; i < count; i++ {
		hosts[i], err = mn.GenPeer()
		require.NoError(t, err)

		f := InitDirectChannelFactory(zap.NewNop(), hosts[i])

		eventBus[i] = eventbus.NewBus()
		emitter, err := pubsub.NewPubSubPayloadEmitter(eventBus[i])
		require.NoError(t, err)

		dc[i], err = f(ctx, emitter, nil)
		require.NoErrorf(t, err, "unable to init directChannelsFactories[%d]", i)
	}

	err = mn.LinkAll()
	require.NoError(t, err)

	err = mn.ConnectAllButSelf()
	require.NoError(t, err)

	ccbus := make([]chan interface{}, len(eventBus))
	subs := make([]event.Subscription, len(eventBus))
	for i, bus := range eventBus {
		subs[i], err = bus.Subscribe(new(iface.EventPubSubPayload))
		require.NoError(t, err)

		ccbus[i] = make(chan interface{}, len(hosts)-1)
		go func(me host.Host, sub event.Subscription, cc chan<- interface{}) {
			defer close(cc)
			for _, h := range hosts {
				if me.ID() == h.ID() {
					continue
				}

				select {
				case <-ctx.Done():
					return
				case e := <-sub.Out():
					cc <- e
				}
			}
		}(hosts[i], subs[i], ccbus[i])
	}

	msent := make(map[string]bool)

	for i := 0; i < count; i++ {
		for j := 0; j < count; j++ {
			if i == j {
				continue
			}

			expectedMessage := fmt.Sprintf("test-%d-%d", i, j)
			msent[expectedMessage] = false

			err = dc[i].Send(ctx, hosts[j].ID(), []byte(expectedMessage))
			if err != io.EOF {
				require.NoError(t, err)
			}
		}
	}

	var evt interface{}
	for i := 0; i < count; i++ {
		for j := 0; j < count; j++ {
			if j == i {
				continue
			}

			select {
			case evt = <-ccbus[j]:
			case <-time.After(time.Second):
				require.Failf(t, "waiting too long", "waiting to long for peer[%d]", i)
			}

			e, ok := evt.(iface.EventPubSubPayload)
			require.True(t, ok)
			expectedMessage := string(e.Payload)

			hasReceive, ok := msent[expectedMessage]
			require.True(t, ok)
			assert.False(t, hasReceive, string(expectedMessage))
			msent[string(expectedMessage)] = true
		}
	}
}
