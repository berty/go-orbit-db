package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores"
	"berty.tech/go-orbit-db/stores/operation"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestReplicateAutomatically(t *testing.T) {
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
		var dbPath1Clean, dbPath2Clean func()
		dbPath1, dbPath1Clean = testingTempDir(t, "db1")
		dbPath2, dbPath2Clean = testingTempDir(t, "db2")

		mocknet := testingMockNet(ctx)

		node1, node1Clean := testingIPFSNode(ctx, t, mocknet)
		node2, node2Clean := testingIPFSNode(ctx, t, mocknet)

		ipfs1 := testingCoreAPI(t, node1)
		ipfs2 := testingCoreAPI(t, node2)

		zap.L().Named("orbitdb.tests").Debug(fmt.Sprintf("node1 is %s", node1.Identity.String()))
		zap.L().Named("orbitdb.tests").Debug(fmt.Sprintf("node2 is %s", node2.Identity.String()))

		_, err := mocknet.LinkPeers(node1.Identity, node2.Identity)
		require.NoError(t, err)

		peerInfo2 := peer.AddrInfo{ID: node2.Identity, Addrs: node2.PeerHost.Addrs()}
		err = ipfs1.Swarm().Connect(ctx, peerInfo2)
		require.NoError(t, err)

		peerInfo1 := peer.AddrInfo{ID: node1.Identity, Addrs: node1.PeerHost.Addrs()}
		err = ipfs2.Swarm().Connect(ctx, peerInfo1)
		require.NoError(t, err)

		orbitdb1, err = orbitdb.NewOrbitDB(ctx, ipfs1, &orbitdb.NewOrbitDBOptions{Directory: &dbPath1})
		require.NoError(t, err)

		orbitdb2, err = orbitdb.NewOrbitDB(ctx, ipfs2, &orbitdb.NewOrbitDBOptions{Directory: &dbPath2})
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		hasAllResults := false

		sub := db2.Subscribe(ctx)
		go func() {
			for evt := range sub {
				switch evt.(type) {
				case *stores.EventReplicated:
					infinity := -1

					result1, err := db1.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					require.NoError(t, err)

					result2, err := db2.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					require.NoError(t, err)

					if len(result1) != len(result2) {
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

		<-ctx.Done()
		require.True(t, hasAllResults)
	})

	t.Run("automatic replication exchanges the correct heads", func(t *testing.T) {
		defer setup(t)()

		entryCount := 5

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

		infinity := -1

		sub1 := db4.Subscribe(ctx)
		go func() {
			for event := range sub1 {
				switch event.(type) {
				case *stores.EventReplicated:
					require.Equal(t, "", "Should not happen")
					subCancel()
				}
			}
		}()

		<-subCtx.Done()

		subCtx, subCancel = context.WithTimeout(ctx, 5*time.Second)
		defer subCancel()

		sub2 := db2.Subscribe(ctx)
		go func() {
			for event := range sub2 {
				switch event.(type) {
				case *stores.EventReplicateProgress:
					e := event.(*stores.EventReplicateProgress)

					op, err := operation.ParseOperation(e.Entry)
					require.NoError(t, err)

					require.Equal(t, op.GetOperation(), "ADD")
					require.Nil(t, op.GetKey())
					require.True(t, strings.HasPrefix(string(op.GetValue()), "hello"))
					require.NotNil(t, e.Entry.GetClock())

				case *stores.EventReplicated:
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
}
