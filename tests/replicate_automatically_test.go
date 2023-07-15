package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	orbitdb "github.com/stateless-minds/go-orbit-db"
	"github.com/stateless-minds/go-orbit-db/accesscontroller"
	"github.com/stateless-minds/go-orbit-db/iface"
	"github.com/stateless-minds/go-orbit-db/stores"
	"github.com/stateless-minds/go-orbit-db/stores/operation"
	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/kubo/core"
	"github.com/libp2p/go-libp2p/core/peer"
	p2pmocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestReplicateAutomatically(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// setup
	var (
		db1, db2           orbitdb.EventLogStore
		node1, node2       *core.IpfsNode
		db3, db4           orbitdb.KeyValueStore
		mocknet            p2pmocknet.Mocknet
		orbitdb1, orbitdb2 iface.OrbitDB
		dbPath1, dbPath2   string
		access             accesscontroller.CreateAccessControllerOptions
	)

	logger := zap.NewNop()

	setup := func(t *testing.T) func() {
		var dbPath1Clean, dbPath2Clean func()
		dbPath1, dbPath1Clean = testingTempDir(t, "db1")
		dbPath2, dbPath2Clean = testingTempDir(t, "db2")

		mocknet = testingMockNet(t)

		var node1Clean, node2Clean func()
		node1, node1Clean = testingIPFSNode(ctx, t, mocknet)
		node2, node2Clean = testingIPFSNode(ctx, t, mocknet)

		ipfs1 := testingCoreAPI(t, node1)
		ipfs2 := testingCoreAPI(t, node2)

		logger.Named("orbitdb.tests").Debug(fmt.Sprintf("node1 is %s", node1.Identity.String()))
		logger.Named("orbitdb.tests").Debug(fmt.Sprintf("node2 is %s", node2.Identity.String()))

		_, err := mocknet.LinkPeers(node1.Identity, node2.Identity)
		require.NoError(t, err)

		orbitdb1, err = orbitdb.NewOrbitDB(ctx, ipfs1, &orbitdb.NewOrbitDBOptions{
			// DirectChannelFactory: directchannel.InitDirectChannelFactory(zap.NewNop(), node1.PeerHost),
			Directory: &dbPath1,
		})
		require.NoError(t, err)

		orbitdb2, err = orbitdb.NewOrbitDB(ctx, ipfs2, &orbitdb.NewOrbitDBOptions{
			// DirectChannelFactory: directchannel.InitDirectChannelFactory(zap.NewNop(), node2.PeerHost),
			Directory: &dbPath2,
		})
		require.NoError(t, err)

		access = accesscontroller.CreateAccessControllerOptions{
			Access: map[string][]string{
				"write": {
					orbitdb1.Identity().ID,
					orbitdb2.Identity().ID,
				},
			},
		}

		db1, err = orbitdb1.Log(ctx, "replicate-automatically-tests", &orbitdb.CreateDBOptions{
			Directory:        &dbPath1,
			AccessController: &access,
		})
		require.NoError(t, err)

		db3, err = orbitdb1.KeyValue(ctx, "replicate-automatically-tests-kv", &orbitdb.CreateDBOptions{
			Directory:        &dbPath1,
			AccessController: &access,
		})
		require.NoError(t, err)

		cleanup := func() {
			db3.Close()
			db3.Drop()
			db1.Close()
			db1.Drop()
			orbitdb1.Close()
			orbitdb2.Close()
			node1Clean()
			node2Clean()
			dbPath1Clean()
			dbPath2Clean()
		}
		return cleanup
	}
	t.Run("starts replicating the database when peers connect", func(t *testing.T) {
		defer setup(t)()

		var err error

		const entryCount = 10

		conn, err := mocknet.ConnectPeers(node1.Identity, node2.Identity)
		require.NoError(t, err)

		db2, err = orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
			Directory:        &dbPath2,
			AccessController: &access,
			Timeout:          time.Second * 5,
		})
		require.NoError(t, err)

		defer db2.Drop()
		defer db2.Close()

		// close connection
		err = conn.Close()
		require.NoError(t, err)

		// add message to log
		ops := make(map[cid.Cid]operation.Operation, entryCount)
		for i := 0; i < entryCount; i++ {
			op, err := db1.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
			require.NoError(t, err)
			ops[op.GetEntry().GetHash()] = op

		}

		sub, err := db2.EventBus().Subscribe(new(stores.EventReplicated))
		require.NoError(t, err)
		defer sub.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		centries := make(chan cid.Cid, entryCount)
		go func() {
			defer close(centries)
			for {
				var evt stores.EventReplicated
				select {
				case e := <-sub.Out():
					evt = e.(stores.EventReplicated)
				case <-ctx.Done():
					return
				}

				for _, entry := range evt.Entries {
					centries <- entry.GetHash()
				}
			}
		}()

		conn, err = mocknet.ConnectPeers(node1.Identity, node2.Identity)
		require.NoError(t, err)
		defer conn.Close()

		for len(ops) > 0 {
			select {
			case <-ctx.Done():
				require.NoError(t, err, "waiting for entries")
			case h := <-centries:
				delete(ops, h)
			}
		}
		cancel()

		infinity := -1
		result1, err := db1.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
		require.NoError(t, err)
		result2, err := db2.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
		require.NoError(t, err)
		require.Equal(t, len(result1), len(result1))

		for i := 0; i < len(result1); i++ {
			require.Equal(t, string(result1[i].GetValue()), string(result2[i].GetValue()))
		}
	})

	t.Run("automatic replication exchanges the correct heads", func(t *testing.T) {
		defer setup(t)()

		entryCount := 5

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var err error

		err = mocknet.ConnectAllButSelf()
		require.NoError(t, err)

		db2, err = orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
			Directory:        &dbPath2,
			AccessController: &access,
		})
		require.NoError(t, err)

		defer db2.Drop()
		defer db2.Close()

		db4, err = orbitdb2.KeyValue(ctx, db3.Address().String(), &orbitdb.CreateDBOptions{
			Directory:        &dbPath2,
			AccessController: &access,
		})
		require.NoError(t, err)

		defer db4.Drop()
		defer db4.Close()

		hasAllResults := false

		sub1, err := db4.EventBus().Subscribe(new(stores.EventReplicated))
		require.NoError(t, err)
		defer sub1.Close()

		select {
		case <-sub1.Out():
			require.Fail(t, "Should not happen")
		default:
		}

		subCtx, subCancel := context.WithTimeout(ctx, 5*time.Second)
		defer subCancel()

		sub2, err := db2.EventBus().Subscribe([]interface{}{
			new(stores.EventReplicateProgress),
			new(stores.EventReplicated),
		})
		require.NoError(t, err)
		defer sub2.Close()

		infinity := -1
		go func() {
			defer cancel()
			for event := range sub2.Out() {
				switch e := event.(type) {
				case stores.EventReplicateProgress:
					op, err := operation.ParseOperation(e.Entry)
					require.NoError(t, err)

					require.Equal(t, op.GetOperation(), "ADD")
					require.Nil(t, op.GetKey())
					require.True(t, strings.HasPrefix(string(op.GetValue()), "hello"))
					require.NotNil(t, e.Entry.GetClock())

				case stores.EventReplicated:
					result1, err := db1.List(subCtx, &orbitdb.StreamOptions{Amount: &infinity})
					require.NoError(t, err)

					result2, err := db2.List(subCtx, &orbitdb.StreamOptions{Amount: &infinity})
					require.NoError(t, err)

					if len(result1) != len(result2) {
						continue
					}

					hasAllResults = true
					for i := 0; i < len(result1); i++ {
						require.Equal(t, string(result1[i].GetValue()), string(result2[i].GetValue()))
					}

					return
				}
			}
		}()

		for i := 0; i < entryCount; i++ {
			_, err := db1.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
			require.NoError(t, err)
		}

		<-subCtx.Done()

		require.True(t, hasAllResults)
	})
}

func TestReplicateAutomaticallyNonMocked(t *testing.T) {
	t.SkipNow()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// setup
	var (
		db1, db2           orbitdb.EventLogStore
		db3, db4           orbitdb.KeyValueStore
		orbitdb1, orbitdb2 iface.OrbitDB
		dbPath1, dbPath2   string
		access             accesscontroller.CreateAccessControllerOptions
	)
	setup := func(t *testing.T) func() {
		var err error
		var dbPath1Clean, dbPath2Clean func()

		apis, cleanupAPIs := testingIPFSAPIsNonMocked(ctx, t, 2)

		dbPath1, dbPath1Clean = testingTempDir(t, "db1")
		dbPath2, dbPath2Clean = testingTempDir(t, "db2")

		orbitdb1, err = orbitdb.NewOrbitDB(ctx, apis[0], &orbitdb.NewOrbitDBOptions{Directory: &dbPath1})
		require.NoError(t, err)

		orbitdb2, err = orbitdb.NewOrbitDB(ctx, apis[1], &orbitdb.NewOrbitDBOptions{Directory: &dbPath2})
		require.NoError(t, err)

		self2, err := apis[1].Key().Self(ctx)
		require.NoError(t, err)

		self2Addrs, err := apis[1].Swarm().LocalAddrs(ctx)
		require.NoError(t, err)

		self1, err := apis[0].Key().Self(ctx)
		require.NoError(t, err)
		self1Addrs, err := apis[0].Swarm().LocalAddrs(ctx)
		require.NoError(t, err)

		peerInfo2 := peer.AddrInfo{ID: self2.ID(), Addrs: self2Addrs}
		err = apis[0].Swarm().Connect(ctx, peerInfo2)
		require.NoError(t, err)

		peerInfo1 := peer.AddrInfo{ID: self1.ID(), Addrs: self1Addrs}
		err = apis[1].Swarm().Connect(ctx, peerInfo1)
		require.NoError(t, err)

		access = accesscontroller.CreateAccessControllerOptions{
			Access: map[string][]string{
				"write": {
					orbitdb1.Identity().ID,
					orbitdb2.Identity().ID,
				},
			},
		}

		db1, err = orbitdb1.Log(ctx, "replicate-automatically-tests", &orbitdb.CreateDBOptions{
			Directory:        &dbPath1,
			AccessController: &access,
		})
		require.NoError(t, err)

		db3, err = orbitdb1.KeyValue(ctx, "replicate-automatically-tests-kv", &orbitdb.CreateDBOptions{
			Directory:        &dbPath1,
			AccessController: &access,
		})
		require.NoError(t, err)

		cleanup := func() {
			db3.Close()
			db3.Drop()
			db1.Close()
			db1.Drop()
			orbitdb1.Close()
			orbitdb2.Close()
			cleanupAPIs()
			dbPath1Clean()
			dbPath2Clean()
		}
		return cleanup
	}

	t.Run("starts replicating the database when peers connect", func(t *testing.T) {
		defer setup(t)()

		const entryCount = 10

		for i := 0; i < entryCount; i++ {
			_, err := db1.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
			require.NoError(t, err)
		}

		var err error
		db2, err = orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
			Directory:        &dbPath2,
			AccessController: &access,
		})
		require.NoError(t, err)

		defer db2.Drop()
		defer db2.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		hasAllResults := false

		sub, err := db2.EventBus().Subscribe(new(stores.EventReplicated))
		require.NoError(t, err)
		defer sub.Close()

		infinity := -1
		go func() {
			for {
				var evt interface{}
				select {
				case evt = <-sub.Out():
				case <-ctx.Done():
					return
				}
				switch evt.(type) {
				case stores.EventReplicated:
					result1, err := db1.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					if !assert.NoError(t, err) {
						return
					}

					result2, err := db2.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					if !assert.NoError(t, err) {
						return
					}

					if len(result1) != len(result2) {
						continue
					}

					hasAllResults = true
					for i := 0; i < len(result1); i++ {
						assert.Equal(t, string(result1[i].GetValue()), string(result2[i].GetValue()))
					}

					cancel()
					return
				}
			}
		}()

		<-ctx.Done()
		require.True(t, hasAllResults)
	})

	t.Run("automatic replication exchanges the correct heads", func(t *testing.T) {
		defer setup(t)()

		entryCount := 100

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var err error
		db2, err = orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
			Directory:        &dbPath2,
			AccessController: &access,
		})
		require.NoError(t, err)

		defer db2.Drop()
		defer db2.Close()

		db4, err = orbitdb2.KeyValue(ctx, db3.Address().String(), &orbitdb.CreateDBOptions{
			Directory:        &dbPath2,
			AccessController: &access,
		})
		require.NoError(t, err)

		defer db4.Drop()
		defer db4.Close()

		subCtx, subCancel := context.WithTimeout(ctx, 5*time.Second)
		defer subCancel()

		hasAllResults := false

		sub1, err := db4.EventBus().Subscribe(new(stores.EventReplicated))
		require.NoError(t, err)
		defer sub1.Close()

		go func() {
			for event := range sub1.Out() {
				switch event.(type) {
				case stores.EventReplicated:
					require.Equal(t, "", "Should not happen")
					subCancel()
				}
			}
		}()

		<-subCtx.Done()

		subCtx, subCancel = context.WithCancel(ctx)
		defer subCancel()

		sub2, err := db2.EventBus().Subscribe([]interface{}{
			new(stores.EventReplicateProgress),
			new(stores.EventReplicated),
		})
		require.NoError(t, err)
		defer sub2.Close()

		infinity := -1
		go func() {
			for event := range sub2.Out() {
				switch e := event.(type) {
				case stores.EventReplicateProgress:
					op, err := operation.ParseOperation(e.Entry)
					require.NoError(t, err)

					require.Equal(t, op.GetOperation(), "ADD")
					require.Nil(t, op.GetKey())
					require.True(t, strings.HasPrefix(string(op.GetValue()), "hello"))
					require.NotNil(t, e.Entry.GetClock())

				case stores.EventReplicated:
					result1, err := db1.List(subCtx, &orbitdb.StreamOptions{Amount: &infinity})
					require.NoError(t, err)

					result2, err := db2.List(subCtx, &orbitdb.StreamOptions{Amount: &infinity})
					require.NoError(t, err)

					if len(result1) != len(result2) || len(result1) != entryCount {
						continue
					}

					hasAllResults = true
					for i := 0; i < len(result1); i++ {
						require.Equal(t, string(result1[i].GetValue()), string(result2[i].GetValue()))
					}

					cancel()
				}
			}
		}()

		for i := 0; i < entryCount; i++ {
			_, err := db1.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
			require.NoError(t, err)
		}

		<-subCtx.Done()
		require.True(t, hasAllResults)
	})

	t.Run("automatic replication exchanges the correct heads - sync after everything written", func(t *testing.T) {
		t.SkipNow()
		defer setup(t)()

		entryCount := 1000

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var err error

		db4, err = orbitdb2.KeyValue(ctx, db3.Address().String(), &orbitdb.CreateDBOptions{
			Directory:        &dbPath2,
			AccessController: &access,
		})
		require.NoError(t, err)

		defer db4.Drop()
		defer db4.Close()

		subCtx, subCancel := context.WithTimeout(ctx, 5*time.Second)
		defer subCancel()

		resultCount := 0

		infinity := -1

		sub1, err := db4.EventBus().Subscribe(new(stores.EventReplicated))
		require.NoError(t, err)
		defer sub1.Close()

		go func() {
			for event := range sub1.Out() {
				switch event.(type) {
				case stores.EventReplicated:
					require.Equal(t, "", "Should not happen")
					subCancel()
				}
			}
		}()

		<-subCtx.Done()

		subCtx, subCancel = context.WithCancel(ctx)
		defer subCancel()

		tRefAdd := time.Now()

		for i := 0; i < entryCount; i++ {
			_, err := db1.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
			require.NoError(t, err)
		}

		t.Logf("Add took: %s", time.Since(tRefAdd).String())

		db2, err = orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
			Directory:        &dbPath2,
			AccessController: &access,
		})
		require.NoError(t, err)

		defer db2.Drop()
		defer db2.Close()

		result1, err := db1.List(subCtx, &orbitdb.StreamOptions{Amount: &infinity})
		require.NoError(t, err)
		require.Equal(t, entryCount, len(result1))

		sub2, err := db2.EventBus().Subscribe([]interface{}{
			new(stores.EventReplicateProgress),
			new(stores.EventReplicated),
		})
		require.NoError(t, err)
		defer sub1.Close()

		go func() {
			for event := range sub2.Out() {
				go func(event interface{}) {
					switch e := event.(type) {
					case stores.EventReplicateProgress:
						op, err := operation.ParseOperation(e.Entry)
						require.NoError(t, err)

						require.Equal(t, op.GetOperation(), "ADD")
						require.Nil(t, op.GetKey())
						require.True(t, strings.HasPrefix(string(op.GetValue()), "hello"))
						require.NotNil(t, e.Entry.GetClock())

					case stores.EventReplicated:
						result2, err := db2.List(subCtx, &orbitdb.StreamOptions{Amount: &infinity})
						require.NoError(t, err)

						resultCount = len(result2)

						if resultCount != entryCount {
							fmt.Printf("replicated: %d/%d\n", resultCount, entryCount)
							return
						}

						for i := 0; i < len(result1); i++ {
							require.Equal(t, string(result1[i].GetValue()), string(result2[i].GetValue()))
						}

						cancel()
					}
				}(event)
			}
		}()

		<-subCtx.Done()
		require.Equal(t, resultCount, entryCount)
	})
}
