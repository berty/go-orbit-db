package tests

import (
	"berty.tech/go-ipfs-log/enc"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	ipfslog "berty.tech/go-ipfs-log"
	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/pubsub/directchannel"
	"berty.tech/go-orbit-db/pubsub/pubsubraw"
	orbitstores "berty.tech/go-orbit-db/stores"
	"berty.tech/go-orbit-db/stores/operation"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
)

func testLogAppendReplicate(t *testing.T, amount int, nodeGen func(t *testing.T, mn mocknet.Mocknet, i int) (orbitdb.OrbitDB, string, func())) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*70)
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*70)
	closeOps = append(closeOps, cancel)

	dbPath1, clean := testingTempDir(t, fmt.Sprintf("db%d", i))
	closeOps = append(closeOps, clean)

	node1, clean := testingIPFSNode(ctx, t, mn)
	closeOps = append(closeOps, clean)

	ipfs1 := testingCoreAPI(t, node1)
	zap.L().Named("orbitdb.tests").Debug(fmt.Sprintf("node%d is %s", i, node1.Identity.String()))

	//loggger, _ := zap.NewDevelopment()
	orbitdb1, err := orbitdb.NewOrbitDB(ctx, ipfs1, &orbitdb.NewOrbitDBOptions{
		Directory: &dbPath1,
		PubSub:    pubsubraw.NewPubSub(node1.PubSub, node1.Identity, nil, nil),
		//Logger:    loggger,
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*70)
	closeOps = append(closeOps, cancel)

	dbPath1, clean := testingTempDir(t, fmt.Sprintf("db%d", i))
	closeOps = append(closeOps, clean)

	node1, clean := testingIPFSNode(ctx, t, mn)
	closeOps = append(closeOps, clean)

	ipfs1 := testingCoreAPI(t, node1)
	zap.L().Named("orbitdb.tests").Debug(fmt.Sprintf("node%d is %s", i, node1.Identity.String()))

	//logger, _ := zap.NewDevelopment()
	logger := zap.NewNop()

	orbitdb1, err := orbitdb.NewOrbitDB(ctx, ipfs1, &orbitdb.NewOrbitDBOptions{
		Directory: &dbPath1,
		Logger:    logger,
	})
	require.NoError(t, err)

	closeOps = append(closeOps, func() { _ = orbitdb1.Close() })

	return orbitdb1, dbPath1, performCloseOps
}

func testLogAppendReplicateMultipeer(t *testing.T, amount int, nodeGen func(t *testing.T, mn mocknet.Mocknet, i int) (orbitdb.OrbitDB, string, func())) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*70)
	defer cancel()
	nitems := amount
	dbs := make([]orbitdb.OrbitDB, nitems)
	dbPaths := make([]string, nitems)
	ids := make([]string, nitems)
	mn := testingMockNet(ctx)

	for i := 0; i < nitems; i++ {
		dbs[i], dbPaths[i], cancel = nodeGen(t, mn, i)
		ids[i] = dbs[i].Identity().ID
		defer cancel()
	}

	err := mn.LinkAll()
	require.NoError(t, err)

	err = mn.ConnectAllButSelf()
	require.NoError(t, err)

	access := &accesscontroller.CreateAccessControllerOptions{
		Access: map[string][]string{
			"write": ids,
		},
	}

	address := "replication-tests"
	stores := make([]orbitdb.EventLogStore, nitems)
	subChans := make([]<-chan events.Event, nitems)

	for i := 0; i < nitems; i++ {
		store, err := dbs[i].Log(ctx, address, &orbitdb.CreateDBOptions{
			Directory:        &dbPaths[i],
			AccessController: access,
		})
		require.NoError(t, err)

		stores[i] = store
		subChans[i] = store.Subscribe(ctx)
		defer func() { _ = store.Close() }()
	}

	<-time.After(5 * time.Second)

	//infinity := -1
	wg := sync.WaitGroup{}
	wg.Add(nitems)
	for i := 0; i < nitems; i++ {
		go func(i int) {
			var err error
			defer wg.Done()
			_, err = stores[i].Add(ctx, []byte(fmt.Sprintf("PingPong")))
			_, err = stores[i].Add(ctx, []byte(fmt.Sprintf("PingPong")))
			_, err = stores[i].Add(ctx, []byte(fmt.Sprintf("PingPong")))
			_, err = stores[i].Add(ctx, []byte(fmt.Sprintf("PingPong")))
			_, err = stores[i].Add(ctx, []byte(fmt.Sprintf("PingPong")))
			_, err = stores[i].Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	wg = sync.WaitGroup{}
	wg.Add(nitems)
	mu := sync.Mutex{}
	received := make([]map[string]bool, nitems)

	for i := 0; i < nitems; i++ {
		go func(i int) {
			defer wg.Done()
			mu.Lock()
			received[i] = make(map[string]bool)
			mu.Unlock()
			storeValue := fmt.Sprintf("hello%d", i)
			for e := range subChans[i] {
				entry := ipfslog.Entry(nil)

				switch evt := e.(type) {
				case *orbitstores.EventWrite:
					entry = evt.Entry

				case *orbitstores.EventReplicateProgress:
					entry = evt.Entry
				}

				if entry == nil {
					continue
				}

				op, _ := operation.ParseOperation(entry)
				if string(op.GetValue()) != storeValue && string(op.GetValue()) != "PingPong" {
					mu.Lock()
					received[i][string(op.GetValue())] = true
					if nitems-1 == len(received[i]) {
						mu.Unlock()
						return
					}
					mu.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()
	ok := true
	mu.Lock()
	for i := 0; i < nitems; i++ {
		if !assert.Equal(t, nitems-1, len(received[i]), fmt.Sprintf("mismatch for client %d", i)) {
			ok = false
		}
	}
	mu.Unlock()
	require.True(t, ok)
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

func TestReplicationMultipeer(t *testing.T) {
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
		2,
		5,
		//6,  //FIXME: need increase test timeout
		//8,  //FIXME: need improve "github.com/libp2p/go-libp2p-pubsub to completely resolve problem + increase test timeout
		//10, //FIXME: need improve "github.com/libp2p/go-libp2p-pubsub to completely resolve problem + increase test timeout
	} {
		for nodeType, nodeGen := range map[string]func(t *testing.T, mn mocknet.Mocknet, i int) (orbitdb.OrbitDB, string, func()){
			"default":        testDefaultNodeGenerator,
			"direct-channel": testDirectChannelNodeGenerator,
			"raw-pubsub":     testRawPubSubNodeGenerator,
		} {
			t.Run(fmt.Sprintf("replicates database of %d entries with node type %s", amount, nodeType), func(t *testing.T) {
				testLogAppendReplicateMultipeer(t, amount, nodeGen)
			})
			time.Sleep(4 * time.Second) // wait some time to let CPU relax
		}
	}
}

func TestLogAppendReplicateEncrypted(t *testing.T) {
	amount := 2
	nodeGen := testDefaultNodeGenerator

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
	defer cancel()

	dbs := make([]orbitdb.OrbitDB, 2)
	dbPaths := make([]string, 2)
	mn := testingMockNet(ctx)

	sharedKey, err := enc.NewSecretbox([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2})
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		dbs[i], dbPaths[i], cancel = nodeGen(t, mn, i)
		defer cancel()
	}

	err = mn.LinkAll()
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
		SharedKey:        sharedKey,
	})
	require.NoError(t, err)

	defer func() { _ = store0.Close() }()

	store1, err := dbs[1].Log(ctx, store0.Address().String(), &orbitdb.CreateDBOptions{
		Directory:        &dbPaths[1],
		AccessController: access,
		SharedKey:        sharedKey,
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

func TestLogAppendReplicateEncryptedWrongKey(t *testing.T) {
	amount := 5
	nodeGen := testDefaultNodeGenerator

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
	defer cancel()

	dbs := make([]orbitdb.OrbitDB, 2)
	dbPaths := make([]string, 2)
	mn := testingMockNet(ctx)

	sharedKey0, err := enc.NewSecretbox([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2})
	require.NoError(t, err)

	sharedKey1, err := enc.NewSecretbox([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 3})
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		dbs[i], dbPaths[i], cancel = nodeGen(t, mn, i)
		defer cancel()
	}

	err = mn.LinkAll()
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
		SharedKey:        sharedKey0,
	})
	require.NoError(t, err)

	defer func() { _ = store0.Close() }()

	store1, err := dbs[1].Log(ctx, store0.Address().String(), &orbitdb.CreateDBOptions{
		Directory:        &dbPaths[1],
		AccessController: access,
		SharedKey:        sharedKey1,
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
	require.Equal(t, 0, len(items))
}
