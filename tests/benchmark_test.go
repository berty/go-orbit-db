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

	ds "github.com/ipfs/go-datastore"
	dsync "github.com/ipfs/go-datastore/sync"
	cfg "github.com/ipfs/kubo/config"
	ipfsCore "github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/coreapi"
	coreiface "github.com/ipfs/kubo/core/coreiface"
	mock "github.com/ipfs/kubo/core/mock"
	"github.com/ipfs/kubo/repo"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func testingRepoB(_ context.Context, b *testing.B) repo.Repo {
	b.Helper()

	c := cfg.Config{}
	priv, pub, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	require.NoError(b, err)

	pid, err := peer.IDFromPublicKey(pub)
	require.NoError(b, err)

	privkeyb, err := crypto.MarshalPrivateKey(priv)
	require.NoError(b, err)

	c.Pubsub.Enabled = cfg.True
	c.Bootstrap = []string{}
	c.Addresses.Swarm = []string{"/ip4/127.0.0.1/tcp/4001", "/ip4/127.0.0.1/udp/4001/quic"}
	c.Identity.PeerID = pid.String()
	c.Identity.PrivKey = base64.StdEncoding.EncodeToString(privkeyb)
	c.Swarm.ResourceMgr.Enabled = cfg.False // we don'b need ressources manager for test

	return &repo.Mock{
		D: dsync.MutexWrap(ds.NewMapDatastore()),
		C: c,
	}
}

func testingIPFSAPIsB(ctx context.Context, b *testing.B, count int) ([]coreiface.CoreAPI, func()) {
	b.Helper()

	mn := testingMockNetB(b)
	defer mn.Close()

	coreAPIs := make([]coreiface.CoreAPI, count)
	cleans := make([]func(), count)

	for i := 0; i < count; i++ {
		node := (*ipfsCore.IpfsNode)(nil)

		node, cleans[i] = testingIPFSNodeB(ctx, b, mn)
		coreAPIs[i] = testingCoreAPIB(b, node)
	}

	return coreAPIs, func() {
		for i := 0; i < count; i++ {
			cleans[i]()
		}
	}
}

func testingIPFSAPIsNonMockedB(ctx context.Context, b *testing.B, count int) ([]coreiface.CoreAPI, func()) {
	b.Helper()

	coreAPIs := make([]coreiface.CoreAPI, count)
	cleans := make([]func(), count)

	for i := 0; i < count; i++ {
		core, err := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{
			Online: true,
			Repo:   testingRepoB(ctx, b),
			ExtraOpts: map[string]bool{
				"pubsub": true,
			},
		})
		require.NoError(b, err)

		coreAPIs[i] = testingCoreAPIB(b, core)
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

func testingIPFSNodeWithoutPubsubB(ctx context.Context, b *testing.B, m mocknet.Mocknet) (*ipfsCore.IpfsNode, func()) {
	b.Helper()

	core, err := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{
		Online: true,
		Repo:   testingRepoB(ctx, b),
		Host:   mock.MockHostOption(m),
		ExtraOpts: map[string]bool{
			"pubsub": false,
		},
	})
	require.NoError(b, err)

	cleanup := func() { core.Close() }
	return core, cleanup
}

func testingIPFSNodeB(ctx context.Context, b *testing.B, m mocknet.Mocknet) (*ipfsCore.IpfsNode, func()) {
	b.Helper()

	core, err := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{
		Online: true,
		Repo:   testingRepoB(ctx, b),
		Host:   mock.MockHostOption(m),
		ExtraOpts: map[string]bool{
			"pubsub": true,
		},
	})
	require.NoError(b, err)

	cleanup := func() { core.Close() }
	return core, cleanup
}

func testingNonMockedIPFSNodeB(ctx context.Context, b *testing.B) (*ipfsCore.IpfsNode, func()) {
	b.Helper()

	core, err := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{
		Online: true,
		Repo:   testingRepoB(ctx, b),
		ExtraOpts: map[string]bool{
			"pubsub": true,
		},
	})
	require.NoError(b, err)

	cleanup := func() { core.Close() }
	return core, cleanup
}

func testingCoreAPIB(b *testing.B, core *ipfsCore.IpfsNode) coreiface.CoreAPI {
	b.Helper()

	api, err := coreapi.NewCoreAPI(core)
	require.NoError(b, err)
	return api
}

func testingMockNetB(b *testing.B) mocknet.Mocknet {
	mn := mocknet.New()
	b.Cleanup(func() { mn.Close() })
	return mn
}

func testingTempDirB(b *testing.B, name string) (string, func()) {
	b.Helper()

	path, err := ioutil.TempDir("", name)
	require.NoError(b, err)

	cleanup := func() { os.RemoveAll(path) }
	return path, cleanup
}
