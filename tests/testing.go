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
)

func init() {
	//zaptest.Level(zapcore.DebugLevel)
	//config := zap.NewDevelopmentConfig()
	//config.OutputPaths = []string{"stdout"}
	//logger, _ := config.Build()
	//zap.ReplaceGlobals(logger)
}

type cleanFunc func()

// TestNetwork is a pointer for the mocked network used in tests

// MakeIPFS Creates a new IPFS node for testing purposes
func testingIPFSNode(ctx context.Context, t *testing.T, m mocknet.Mocknet) (*ipfsCore.IpfsNode, cleanFunc) {
	t.Helper()

	core, err := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{
		Online: true,
		Host:   mock.MockHostOption(m),
		ExtraOpts: map[string]bool{
			"pubsub": true,
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	return core, func() {
		core.Close()
	}
}

func testingCoreAPI(t *testing.T, core *ipfsCore.IpfsNode) (api iface.CoreAPI) {
	t.Helper()

	var err error
	if api, err = coreapi.NewCoreAPI(core); err != nil {
		t.Fatal(err)
	}

	return
}

func testingMockNet(ctx context.Context) mocknet.Mocknet {
	return mocknet.New(ctx)
}

func testingTempDir(t *testing.T, name string) (string, cleanFunc) {
	t.Helper()

	path, err := ioutil.TempDir("", name)
	if err != nil {
		t.Fatal(err)
	}

	return path, func() {
		os.RemoveAll(path)
	}
}
