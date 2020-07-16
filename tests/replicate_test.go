package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/pubsub/directchannel"
	"berty.tech/go-orbit-db/pubsub/pubsubraw"
)

func testLogAppendReplicate(t *testing.T, amount int, nodeGen func(t *testing.T, mn mocknet.Mocknet, i int) (orbitdb.OrbitDB, string, func())) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	dbs := make([]orbitdb.OrbitDB, 2)
	dbPaths := make([]string, 2)
	mn := testingMockNet(ctx)

	for i := 0; i < 2; i++ {
		dbs[i], dbPaths[i], cancel = nodeGen(t, mn, i)
		defer cancel()
	}

	err := mn.LinkAll()
	require.NoError(t, err)

	err = mn.ConnectAllButSelf()
	require.NoError(t, err)

	access := &accesscontroller.CreateAccessControllerOptions{
		Access: map[string][]string{
			"write": {
				dbs[0].Identity().ID,
				dbs[1].Identity().ID,
			},
		},
	}

	store0, err := dbs[0].Log(ctx, "replication-tests", &orbitdb.CreateDBOptions{
		Directory:        &dbPaths[0],
		AccessController: access,
	})
	require.NoError(t, err)

	defer func() { _ = store0.Close() }()

	store1, err := dbs[1].Log(ctx, store0.Address().String(), &orbitdb.CreateDBOptions{
		Directory:        &dbPaths[1],
		AccessController: access,
	})
	require.NoError(t, err)

	defer func() { _ = store1.Close() }()

	infinity := -1

	for i := 0; i < amount; i++ {
		_, err = store0.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
		require.NoError(t, err)
	}

	items, err := store0.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
	require.NoError(t, err)
	require.Equal(t, amount, len(items))

	<-time.After(time.Millisecond * 2000)
	items, err = store1.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
	require.NoError(t, err)
	require.Equal(t, amount, len(items))
	require.Equal(t, "hello0", string(items[0].GetValue()))
	require.Equal(t, fmt.Sprintf("hello%d", amount-1), string(items[len(items)-1].GetValue()))
}

func testDirectChannelNodeGenerator(t *testing.T, mn mocknet.Mocknet, i int) (orbitdb.OrbitDB, string, func()) {
	var closeOps []func()

	performCloseOps := func() {
		for i := len(closeOps) - 1; i >= 0; i-- {
			closeOps[i]()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	closeOps = append(closeOps, cancel)

	dbPath1, clean := testingTempDir(t, fmt.Sprintf("db%d", i))
	closeOps = append(closeOps, clean)

	node1, clean := testingIPFSNode(ctx, t, mn)
	closeOps = append(closeOps, clean)

	ipfs1 := testingCoreAPI(t, node1)
	zap.L().Named("orbitdb.tests").Debug(fmt.Sprintf("node%d is %s", i, node1.Identity.String()))

	orbitdb1, err := orbitdb.NewOrbitDB(ctx, ipfs1, &orbitdb.NewOrbitDBOptions{
		Directory:            &dbPath1,
		DirectChannelFactory: directchannel.InitDirectChannelFactory(node1.PeerHost),
	})
	require.NoError(t, err)

	closeOps = append(closeOps, func() { _ = orbitdb1.Close() })

	return orbitdb1, dbPath1, performCloseOps
}

func testRawPubSubNodeGenerator(t *testing.T, mn mocknet.Mocknet, i int) (orbitdb.OrbitDB, string, func()) {
	var closeOps []func()

	performCloseOps := func() {
		for i := len(closeOps) - 1; i >= 0; i-- {
			closeOps[i]()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	closeOps = append(closeOps, cancel)

	dbPath1, clean := testingTempDir(t, fmt.Sprintf("db%d", i))
	closeOps = append(closeOps, clean)

	node1, clean := testingIPFSNode(ctx, t, mn)
	closeOps = append(closeOps, clean)

	ipfs1 := testingCoreAPI(t, node1)
	zap.L().Named("orbitdb.tests").Debug(fmt.Sprintf("node%d is %s", i, node1.Identity.String()))

	orbitdb1, err := orbitdb.NewOrbitDB(ctx, ipfs1, &orbitdb.NewOrbitDBOptions{
		Directory: &dbPath1,
		PubSub:    pubsubraw.NewPubSub(node1.PubSub, node1.Identity, nil, nil),
	})
	require.NoError(t, err)

	closeOps = append(closeOps, func() { _ = orbitdb1.Close() })

	return orbitdb1, dbPath1, performCloseOps
}

func testDefaultNodeGenerator(t *testing.T, mn mocknet.Mocknet, i int) (orbitdb.OrbitDB, string, func()) {
	var closeOps []func()

	performCloseOps := func() {
		for i := len(closeOps) - 1; i >= 0; i-- {
			closeOps[i]()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	closeOps = append(closeOps, cancel)

	dbPath1, clean := testingTempDir(t, fmt.Sprintf("db%d", i))
	closeOps = append(closeOps, clean)

	node1, clean := testingIPFSNode(ctx, t, mn)
	closeOps = append(closeOps, clean)

	ipfs1 := testingCoreAPI(t, node1)
	zap.L().Named("orbitdb.tests").Debug(fmt.Sprintf("node%d is %s", i, node1.Identity.String()))

	orbitdb1, err := orbitdb.NewOrbitDB(ctx, ipfs1, &orbitdb.NewOrbitDBOptions{
		Directory: &dbPath1,
	})
	require.NoError(t, err)

	closeOps = append(closeOps, func() { _ = orbitdb1.Close() })

	return orbitdb1, dbPath1, performCloseOps
}

func TestReplication(t *testing.T) {
	if os.Getenv("WITH_GOLEAK") == "1" {
		defer goleak.VerifyNone(t,
			goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),           // inherited from one of the imports (init)
			goleak.IgnoreTopFunction("github.com/ipfs/go-log/writer.(*MirrorWriter).logRoutine"),       // inherited from one of the imports (init)
			goleak.IgnoreTopFunction("github.com/libp2p/go-libp2p-connmgr.(*BasicConnMgr).background"), // inherited from github.com/ipfs/go-ipfs/core.NewNode
			goleak.IgnoreTopFunction("github.com/jbenet/goprocess/periodic.callOnTicker.func1"),        // inherited from github.com/ipfs/go-ipfs/core.NewNode
			goleak.IgnoreTopFunction("github.com/libp2p/go-libp2p-connmgr.(*decayer).process"),         // inherited from github.com/ipfs/go-ipfs/core.NewNode)
			goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),                    // inherited from github.com/ipfs/go-ipfs/core.NewNode)
		)
	}

	for _, amount := range []int{
		1,
		10,
		// 100,
	} {
		for nodeType, nodeGen := range map[string]func(t *testing.T, mn mocknet.Mocknet, i int) (orbitdb.OrbitDB, string, func()){
			"default":        testDefaultNodeGenerator,
			"direct-channel": testDirectChannelNodeGenerator,
			"raw-pubsub":     testRawPubSubNodeGenerator,
		} {
			t.Run(fmt.Sprintf("replicates database of %d entries with node type %s", amount, nodeType), func(t *testing.T) {
				testLogAppendReplicate(t, amount, nodeGen)
			})
		}
	}
}
