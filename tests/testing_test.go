package tests

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	ipfsCore "github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/coreapi"
	mock "github.com/ipfs/go-ipfs/core/mock"
	iface "github.com/ipfs/interface-go-ipfs-core"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

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
