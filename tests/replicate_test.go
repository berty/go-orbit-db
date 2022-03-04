package tests

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/enc"
	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/pubsub/directchannel"
	"berty.tech/go-orbit-db/pubsub/pubsubraw"
	orbitstores "berty.tech/go-orbit-db/stores"
	"berty.tech/go-orbit-db/stores/operation"
	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
)

func TestLogAppendReplicateEncrypted(t *testing.T) {
	amount := 2
	nodeGen := testDefaultNodeGenerator

	ctx, cancel := context.WithCancel(context.Background())
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

	sub, err := store1.EventBus().Subscribe([]interface{}{
		new(orbitstores.EventReplicated),
	})
	require.NoError(t, err)
	defer sub.Close()

	cerr := make(chan error)
	go func() {
		for replicated := 0; replicated < amount; {
			select {
			case <-time.After(time.Second * 10):
				cerr <- fmt.Errorf("timeout while waiting for event, replicated event: %d", replicated)
				return
			case evt := <-sub.Out():
				replicated += evt.(orbitstores.EventReplicated).LogLength
			}
		}

		cerr <- nil
	}()

	infinity := -1

	for i := 0; i < amount; i++ {
		_, err = store0.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
		require.NoError(t, err)
	}

	items, err := store0.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
	require.NoError(t, err)
	require.Equal(t, amount, len(items))

	err = <-cerr
	assert.NoError(t, err)

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

	sub, err := store1.EventBus().Subscribe([]interface{}{
		new(orbitstores.EventReplicated),
	})
	require.NoError(t, err)

	for i := 0; i < amount; i++ {
		_, err = store0.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
		require.NoError(t, err)
	}

	items, err := store0.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
	require.NoError(t, err)
	require.Equal(t, amount, len(items))

	sub.Close()

	select {
	case e := <-sub.Out():
		assert.Nil(t, e)
	default:
	}

	items, err = store1.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
	require.NoError(t, err)
	require.Equal(t, 0, len(items))
}

func TestReplication(t *testing.T) {
	// err := log.SetLogLevel("pubsub", "debug")
	// require.NoError(t, err)

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

	var cases []int
	if os.Getenv("SKIP_SLOW") == "1" {
		cases = []int{1, 10}
	} else {
		cases = []int{1, 10, 100}
	}

	for _, amount := range cases {
		for nodeType, nodeGen := range map[string]func(t *testing.T, mn mocknet.Mocknet, i int) (orbitdb.OrbitDB, string, func()){
			"default":        testDefaultNodeGenerator,
			"direct-channel": testDirectChannelNodeGenerator,
		} {
			t.Run(fmt.Sprintf("replicates database of %d entries with node type %s", amount, nodeType), func(t *testing.T) {
				testLogAppendReplicate(t, amount, nodeGen)
			})
		}
	}
}

func testLogAppendReplicate(t *testing.T, amount int, nodeGen func(t *testing.T, mn mocknet.Mocknet, i int) (orbitdb.OrbitDB, string, func())) {
	type replicateEvent int
	const (
		EventReplicate replicateEvent = iota
		EventReplicateProgress
		EventReplicated
	)

	ctx, cancel := context.WithCancel(context.Background())
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

	sub, err := store1.EventBus().Subscribe([]interface{}{
		new(orbitstores.EventReplicateProgress),
		new(orbitstores.EventReplicated),
	}, eventbus.BufSize(amount+32))

	require.NoError(t, err)
	defer sub.Close()

	events := make(map[replicateEvent]int)
	cerr := make(chan error)
	go func() {
		defer close(cerr)
		for events[EventReplicated] < amount || events[EventReplicateProgress] < amount {
			var e interface{}
			select {
			case <-ctx.Done():
				cerr <- ctx.Err()
				return
			case <-time.After(time.Second * 10):
				cerr <- fmt.Errorf("timeout while waiting for event")
				return
			case e = <-sub.Out():
			}

			switch evt := e.(type) {
			case orbitstores.EventReplicateProgress:
				events[EventReplicateProgress] += 1
			case orbitstores.EventReplicated:
				events[EventReplicated] += evt.LogLength
			}
		}
	}()

	for i := 0; i < amount; i++ {
		_, err = store0.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
		require.NoError(t, err)
	}

	items, err := store0.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
	require.NoError(t, err)
	require.Equal(t, amount, len(items))

	err = <-cerr

	evtReplicateProgress, ok := events[EventReplicateProgress]
	if assert.True(t, ok) {
		assert.Equal(t, amount, evtReplicateProgress, "EventReplicateProgress")
	}

	evtReplicated, ok := events[EventReplicated]
	if assert.True(t, ok) {
		assert.Equal(t, amount, evtReplicated, "EventReplicated")
	}

	assert.NoError(t, err)

	items, err = store1.List(context.Background(), &orbitdb.StreamOptions{Amount: &infinity})
	require.NoError(t, err)
	require.Equal(t, amount, len(items))
	require.Equal(t, "hello0", string(items[0].GetValue()))
	require.Equal(t, fmt.Sprintf("hello%d", amount-1), string(items[len(items)-1].GetValue()))
}

func TestReplicationMultipeer(t *testing.T) {
	// err := log.SetLogLevel("pubsub", "debug")
	// require.NoError(t, err)

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

	var cases []int
	if os.Getenv("SKIP_SLOW") == "1" {
		cases = []int{2, 3}
	} else {
		cases = []int{2, 5, 10}
	}

	for _, amount := range cases {
		for nodeType, nodeGen := range map[string]func(t *testing.T, mn mocknet.Mocknet, i int) (orbitdb.OrbitDB, string, func()){
			"default":        testDefaultNodeGenerator,
			"direct-channel": testDirectChannelNodeGenerator,
		} {
			t.Run(fmt.Sprintf("replicates database of %d entries with node type %s", amount, nodeType), func(t *testing.T) {
				testLogAppendReplicateMultipeer(t, amount, nodeGen)
			})
		}
	}
}

func testLogAppendReplicateMultipeer(t *testing.T, npeer int, nodeGen func(t *testing.T, mn mocknet.Mocknet, i int) (orbitdb.OrbitDB, string, func())) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const nitems = 5

	dbs := make([]orbitdb.OrbitDB, npeer)
	dbPaths := make([]string, npeer)
	ids := make([]string, npeer)
	mn := testingMockNet(ctx)

	var cleanup func()
	for i := 0; i < npeer; i++ {
		dbs[i], dbPaths[i], cleanup = nodeGen(t, mn, i)
		ids[i] = dbs[i].Identity().ID
		defer cleanup()
	}

	err := mn.LinkAll()
	require.NoError(t, err)

	// connect peers
	err = mn.ConnectAllButSelf()
	require.NoError(t, err)

	access := &accesscontroller.CreateAccessControllerOptions{
		Access: map[string][]string{
			"write": ids,
		},
	}

	address := "replication-tests"
	stores := make([]orbitdb.EventLogStore, npeer)
	subChans := make([]event.Subscription, npeer)
	nToReceive := npeer * nitems

	for i := 0; i < npeer; i++ {
		store, err := dbs[i].Log(ctx, address, &orbitdb.CreateDBOptions{
			Directory:        &dbPaths[i],
			AccessController: access,
			Timeout:          time.Second * 10,
		})
		require.NoError(t, err)

		stores[i] = store
		subChans[i], err = store.EventBus().Subscribe([]interface{}{
			new(orbitstores.EventReplicated),
			new(orbitstores.EventWrite),
			new(orbitstores.EventReplicateProgress),
		}, eventbus.BufSize(nToReceive))
		require.NoError(t, err)

		defer func(i int) {
			subChans[i].Close()
			_ = store.Close()
		}(i)
	}

	centries := make([]chan ipfslog.Entry, npeer)
	for i := 0; i < npeer; i++ {
		centries[i] = make(chan ipfslog.Entry, nToReceive)
		go func(i int) {
			defer close(centries[i])

			var replicated, write, progress int
			for replicated < (nToReceive-nitems) || write+progress < nToReceive {
				var e interface{}

				select {
				case e = <-subChans[i].Out():
				case <-time.After(time.Second * 20):
					assert.Equal(t, nToReceive-nitems, replicated,
						"didn't received enough replicated events")
					assert.Equal(t, nToReceive-nitems, progress,
						"didn't received enough progress events")
					assert.Equal(t, nitems, write,
						"didn't received enough write events")
					cancel()
					assert.NoError(t, ctx.Err())
					return
				}

				if e == nil {
					assert.Fail(t, "receiving nil entry")
					return
				}

				switch evt := e.(type) {
				case orbitstores.EventReplicateProgress:
					centries[i] <- evt.Entry
					progress += 1
				case orbitstores.EventWrite:
					centries[i] <- evt.Entry
					write += 1
				case orbitstores.EventReplicated:
					replicated += evt.LogLength
				}
			}
		}(i)
	}

	payloads := make([]string, nToReceive)
	wg := sync.WaitGroup{}

	wg.Add(npeer)

	for i := 0; i < npeer; i++ {
		go func(i int) {
			var err error
			for j := 0; j < nitems; j++ {
				msg := fmt.Sprintf("[%d]entry-%d", i, j)
				eventid := i*nitems + j
				payloads[eventid] = msg
				_, err = stores[i].Add(ctx, []byte(msg))
				require.NoError(t, err)
			}

			wg.Done()
		}(i)
	}

	wg.Wait()

	received := make([]map[string]int, npeer)
	for i := 0; i < npeer; i++ {
		received[i] = make(map[string]int)
		for entry := range centries[i] {
			op, err := operation.ParseOperation(entry)
			require.NoError(t, err)
			value := string(op.GetValue())
			received[i][value] += 1
		}
	}

	// check if every entries has been received
	infinity := -1
	for i, peer := range received {
		for _, payload := range payloads {
			n, ok := peer[payload]
			if assert.Truef(t, ok, "peer %d missing entry `%s`", i, payload) {
				assert.Equalf(t, 1, n, "stores[%d] should be receive entry `%s` only once", i, payload)
			}
		}

		items, err := stores[i].List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
		require.NoError(t, err)
		assert.Equal(t, nToReceive, len(items), "stores[%d] should have %d item(s), but have %d", i, nToReceive, len(items))
	}
}

func testDirectChannelNodeGenerator(t *testing.T, mn mocknet.Mocknet, i int) (orbitdb.OrbitDB, string, func()) {
	var closeOps []func()

	performCloseOps := func() {
		for i := len(closeOps) - 1; i >= 0; i-- {
			closeOps[i]()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	closeOps = append(closeOps, cancel)

	dbPath1, clean := testingTempDir(t, fmt.Sprintf("db%d", i))
	closeOps = append(closeOps, clean)

	node1, clean := testingIPFSNode(ctx, t, mn)
	closeOps = append(closeOps, clean)

	// logger, _ := zap.NewDevelopment()
	logger := zap.NewNop()

	ipfs1 := testingCoreAPI(t, node1)
	logger.Named("orbitdb.tests").Debug(fmt.Sprintf("node%d is %s", i, node1.Identity.String()))

	orbitdb1, err := orbitdb.NewOrbitDB(ctx, ipfs1, &orbitdb.NewOrbitDBOptions{
		Directory:            &dbPath1,
		DirectChannelFactory: directchannel.InitDirectChannelFactory(zap.NewNop(), node1.PeerHost),
		// @NOTE(gfanton): use raw pubsub here, we need a higher buffer
		// for subscribe to make the test works on CI
		PubSub: pubsubraw.NewPubSub(node1.PubSub, node1.Identity, nil, nil),
		Logger: logger,
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

	// logger, _ := zap.NewDevelopment()
	logger := zap.NewNop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*70)
	closeOps = append(closeOps, cancel)

	dbPath1, clean := testingTempDir(t, fmt.Sprintf("db%d", i))
	closeOps = append(closeOps, clean)

	node1, clean := testingIPFSNode(ctx, t, mn)
	closeOps = append(closeOps, clean)

	ipfs1 := testingCoreAPI(t, node1)
	logger.Named("orbitdb.tests").Debug(fmt.Sprintf("node%d is %s", i, node1.Identity.String()))

	orbitdb1, err := orbitdb.NewOrbitDB(ctx, ipfs1, &orbitdb.NewOrbitDBOptions{
		// @NOTE(gfanton): use raw pubsub here, we need a higher buffer
		// for subscribe to make the test works on CI
		PubSub:    pubsubraw.NewPubSub(node1.PubSub, node1.Identity, logger, nil),
		Directory: &dbPath1,
		Logger:    logger,
	})
	require.NoError(t, err)

	closeOps = append(closeOps, func() { _ = orbitdb1.Close() })

	return orbitdb1, dbPath1, performCloseOps
}
