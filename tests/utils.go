package tests

import (
	"context"
	ipfsCore "github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/coreapi"
	mock "github.com/ipfs/go-ipfs/core/mock"
	iface "github.com/ipfs/interface-go-ipfs-core"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/pkg/errors"
	"github.com/shibukawa/configdir"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"testing"
)

func init() {
	zaptest.Level(zapcore.DebugLevel)
	config := zap.NewDevelopmentConfig()
	config.OutputPaths = []string{"stdout"}
	logger, _ := config.Build()
	zap.ReplaceGlobals(logger)
}

func getTempDirectory() string {
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

var testNetwork mocknet.Mocknet

func makeIPFS(ctx context.Context, t *testing.T) (*ipfsCore.IpfsNode, iface.CoreAPI) {
	if testNetwork == nil {
		testNetwork = mocknet.New(ctx)
	}

	core, err := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{
		Online: true,
		Host:   mock.MockHostOption(testNetwork),
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

func teardownNetwork () {
	testNetwork = nil
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}
