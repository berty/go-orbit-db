package tests

import (
	"context"
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

// TestNetwork is a pointer for the mocked network used in tests
var TestNetwork mocknet.Mocknet

// MakeIPFS Creates a new IPFS node for testing purposes
func MakeIPFS(ctx context.Context, t *testing.T) (*ipfsCore.IpfsNode, iface.CoreAPI) {
	if TestNetwork == nil {
		TestNetwork = mocknet.New(ctx)
	}

	core, err := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{
		Online: true,
		Host:   mock.MockHostOption(TestNetwork),
		ExtraOpts: map[string]bool{
			"pubsub": true,
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	api, err := coreapi.NewCoreAPI(core)
	if err != nil {
		t.Fatal(err)
	}

	return core, api
}

// TeardownNetwork Clears the mock net pointer
func TeardownNetwork() {
	TestNetwork = nil
}
