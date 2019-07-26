package tests

import (
	"context"
	ipfsCore "github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/coreapi"
	mock "github.com/ipfs/go-ipfs/core/mock"
	iface "github.com/ipfs/interface-go-ipfs-core"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/shibukawa/configdir"
	"testing"
)

func init() {
	//zaptest.Level(zapcore.DebugLevel)
	//config := zap.NewDevelopmentConfig()
	//config.OutputPaths = []string{"stdout"}
	//logger, _ := config.Build()
	//zap.ReplaceGlobals(logger)
}

func GetTempDirectory() string {
	storagePath := configdir.New("go-orbit-db", "go-orbit-db")
	storageDirs := storagePath.QueryFolders(configdir.Cache)

	if len(storageDirs) == 0 {
		panic("no storage path found")
	}

	if err := storageDirs[0].CreateParentDir(""); err != nil {
		panic(err.Error())
	}

	return storageDirs[0].Path
}

var TestNetwork mocknet.Mocknet

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

func TeardownNetwork() {
	TestNetwork = nil
}
