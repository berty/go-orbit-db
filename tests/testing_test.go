package tests

import (
	"context"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"io/ioutil"
	"os"
	"testing"

	ipfsCore "github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/coreapi"
	mock "github.com/ipfs/go-ipfs/core/mock"
	ipfsLibP2P "github.com/ipfs/go-ipfs/core/node/libp2p"
	iface "github.com/ipfs/interface-go-ipfs-core"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func testingIPFSAPIs(ctx context.Context, t *testing.T, count int) ([]iface.CoreAPI, func()) {
	t.Helper()

	mn := testingMockNet(ctx)
	coreAPIs := make([]iface.CoreAPI, count)
	cleans := make([]func(), count)

	for i := 0; i < count; i++ {
		node := (*ipfsCore.IpfsNode)(nil)

		node, cleans[i] = testingIPFSNode(ctx, t, mn)
		coreAPIs[i] = testingCoreAPI(t, node)
	}

	return coreAPIs, func() {
		for i := 0; i < count; i++ {
			cleans[i]()
		}
	}
}

func testingIPFSAPIsNonMocked(ctx context.Context, t *testing.T, count int) ([]iface.CoreAPI, func()) {
	t.Helper()

	coreAPIs := make([]iface.CoreAPI, count)
	cleans := make([]func(), count)

	for i := 0; i < count; i++ {
		core, err := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{
			Online: true,
			ExtraOpts: map[string]bool{
				"pubsub": true,
			},
			Host: func(ctx context.Context, id peer.ID, ps peerstore.Peerstore, options ...libp2p.Option) (host.Host, error) {
				options = append(options, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
				return ipfsLibP2P.DefaultHostOption(ctx, id, ps, options...)
			},
		})
		require.NoError(t, err)

		coreAPIs[i] = testingCoreAPI(t, core)
		cleans[i] = func() {
			core.Close()
		}
	}

	return coreAPIs, func() {
		for i := 0; i < count; i++ {
			cleans[i]()
		}
	}
}

func testingIPFSNode(ctx context.Context, t *testing.T, m mocknet.Mocknet) (*ipfsCore.IpfsNode, func()) {
	t.Helper()

	core, err := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{
		Online: true,
		Host:   mock.MockHostOption(m),
		ExtraOpts: map[string]bool{
			"pubsub": true,
		},
	})
	require.NoError(t, err)

	cleanup := func() { core.Close() }
	return core, cleanup
}

func testingNonMockedIPFSNode(ctx context.Context, t *testing.T) (*ipfsCore.IpfsNode, func()) {
	t.Helper()

	core, err := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{
		Online: true,
		ExtraOpts: map[string]bool{
			"pubsub": true,
		},
	})
	require.NoError(t, err)

	cleanup := func() { core.Close() }
	return core, cleanup
}

func testingCoreAPI(t *testing.T, core *ipfsCore.IpfsNode) iface.CoreAPI {
	t.Helper()

	api, err := coreapi.NewCoreAPI(core)
	require.NoError(t, err)
	return api
}

func testingMockNet(ctx context.Context) mocknet.Mocknet {
	return mocknet.New(ctx)
}

func testingTempDir(t *testing.T, name string) (string, func()) {
	t.Helper()

	path, err := ioutil.TempDir("", name)
	require.NoError(t, err)

	cleanup := func() { os.RemoveAll(path) }
	return path, cleanup
}
