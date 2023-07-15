package tests

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"io/ioutil"
	"os"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	iface "github.com/ipfs/boxo/coreiface"
	ds "github.com/ipfs/go-datastore"
	dsync "github.com/ipfs/go-datastore/sync"
	cfg "github.com/ipfs/kubo/config"
	ipfsCore "github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/coreapi"
	mock "github.com/ipfs/kubo/core/mock"
	"github.com/ipfs/kubo/repo"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func testingRepo(ctx context.Context, t *testing.T) repo.Repo {
	t.Helper()

	c := cfg.Config{}
	priv, pub, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	require.NoError(t, err)

	pid, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	privkeyb, err := crypto.MarshalPrivateKey(priv)
	require.NoError(t, err)

	c.Pubsub.Enabled = cfg.True
	c.Bootstrap = []string{}
	c.Addresses.Swarm = []string{"/ip4/127.0.0.1/tcp/4001", "/ip4/127.0.0.1/udp/4001/quic"}
	c.Identity.PeerID = pid.Pretty()
	c.Identity.PrivKey = base64.StdEncoding.EncodeToString(privkeyb)
	c.Swarm.ResourceMgr.Enabled = cfg.False // we don't need ressources manager for test

	return &repo.Mock{
		D: dsync.MutexWrap(ds.NewMapDatastore()),
		C: c,
	}
}

func testingIPFSAPIs(ctx context.Context, t *testing.T, count int) ([]iface.CoreAPI, func()) {
	t.Helper()

	mn := testingMockNet(t)
	defer mn.Close()

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
			Repo:   testingRepo(ctx, t),
			ExtraOpts: map[string]bool{
				"pubsub": true,
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

func testingIPFSNodeWithoutPubsub(ctx context.Context, t *testing.T, m mocknet.Mocknet) (*ipfsCore.IpfsNode, func()) {
	t.Helper()

	core, err := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{
		Online: true,
		Repo:   testingRepo(ctx, t),
		Host:   mock.MockHostOption(m),
		ExtraOpts: map[string]bool{
			"pubsub": false,
		},
	})
	require.NoError(t, err)

	cleanup := func() { core.Close() }
	return core, cleanup
}

func testingIPFSNode(ctx context.Context, t *testing.T, m mocknet.Mocknet) (*ipfsCore.IpfsNode, func()) {
	t.Helper()

	core, err := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{
		Online: true,
		Repo:   testingRepo(ctx, t),
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
		Repo:   testingRepo(ctx, t),
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

func testingMockNet(t *testing.T) mocknet.Mocknet {
	mn := mocknet.New()
	t.Cleanup(func() { mn.Close() })
	return mn
}

func testingTempDir(t *testing.T, name string) (string, func()) {
	t.Helper()

	path, err := ioutil.TempDir("", name)
	require.NoError(t, err)

	cleanup := func() { os.RemoveAll(path) }
	return path, cleanup
}
