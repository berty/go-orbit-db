package directchannel

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"berty.tech/go-orbit-db/iface"
	"github.com/libp2p/go-libp2p-core/host"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestInitDirectChannelFactory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	mn := mocknet.New(ctx)

	var err error
	count := 10
	hosts := make([]host.Host, count)
	directChannelsFactories := make([]iface.DirectChannelFactory, count)

	for i := 0; i < count; i++ {
		hosts[i], err = mn.GenPeer()
		require.NoError(t, err)

		directChannelsFactories[i] = InitDirectChannelFactory(ctx, zap.NewNop(), hosts[i])
	}

	err = mn.LinkAll()
	require.NoError(t, err)

	err = mn.ConnectAllButSelf()
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	require.GreaterOrEqual(t, count, 2)

	var (
		receivedMessages uint32 = 0
		expectedMessages uint32 = 0
	)

	for i := 0; i < count; i++ {
		for j := i + 1; j < count; j++ {
			wg.Add(1)
			expectedMessages++

			go func(i, j int) {
				ctx, cancel := context.WithCancel(ctx)

				subWg := sync.WaitGroup{}
				subWg.Add(2)

				expectedMessage := []byte(fmt.Sprintf("test-%d-%d", i, j))

				ch1, err := directChannelsFactories[j](ctx, hosts[i].ID(), nil)
				assert.NoError(t, err)

				ch2, err := directChannelsFactories[i](ctx, hosts[j].ID(), nil)
				assert.NoError(t, err)

				go func() {
					err := ch1.Connect(ctx)
					assert.NoError(t, err)

					subWg.Done()
				}()

				go func() {
					err := ch2.Connect(ctx)
					assert.NoError(t, err)

					subWg.Done()
				}()

				subWg.Wait()

				var valOK uint32 = 1

				subWg.Add(1)

				go func() {
					defer cancel()

					sub := ch2.GlobalChannel(ctx)
					subWg.Done()

					for evt := range sub {
						if e, ok := evt.(*iface.EventPubSubPayload); ok {
							if bytes.Equal(e.Payload, expectedMessage) {
								count := atomic.AddUint32(&receivedMessages, 1)
								t.Log(fmt.Sprintf("successfully received message from %d for %d (%d)", i, j, count))
								atomic.StoreUint32(&valOK, 1)
								return
							}
						}
					}

					t.Log(fmt.Sprintf("failed to receive message from %d for %d", i, j))
					atomic.StoreUint32(&valOK, 1)
				}()

				subWg.Wait()

				err = ch1.Send(ctx, expectedMessage)
				if err != io.EOF {
					assert.NoError(t, err)
				}

				<-ctx.Done()
				wg.Done()

				if atomic.LoadUint32(&valOK) == 0 {
					t.Log(fmt.Sprintf("wtf from %d for %d", i, j))
				}
			}(i, j)
		}
	}

	go func() {
		wg.Wait()
		cancel()
	}()

	<-ctx.Done()
	if !assert.Equal(t, int(expectedMessages), int(atomic.LoadUint32(&receivedMessages))) {
		time.Sleep(time.Second * 10)
	}
}
