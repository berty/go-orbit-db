package tests

import (
	"context"
	"testing"
	"time"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores"
	"berty.tech/go-orbit-db/stores/basestore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicationStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// setup
	var (
		db, db2            orbitdb.EventLogStore
		orbitdb1, orbitdb2 iface.OrbitDB
	)
	setup := func(t *testing.T) func() {
		t.Helper()

		dbPath1, dbPath1Clean := testingTempDir(t, "db1")
		require.NotEmpty(t, dbPath1)

		dbPath2, dbPath2Clean := testingTempDir(t, "db2")
		require.NotEmpty(t, dbPath2)

		mocknet := testingMockNet(ctx)
		require.NotNil(t, mocknet)

		node, nodeClean := testingIPFSNode(ctx, t, mocknet)
		require.NotNil(t, node)

		ipfs := testingCoreAPI(t, node)
		require.NotNil(t, ipfs)

		var err error
		orbitdb1, err = orbitdb.NewOrbitDB(ctx, ipfs, &orbitdb.NewOrbitDBOptions{Directory: &dbPath1})
		require.NoError(t, err)
		require.NotNil(t, orbitdb1)

		orbitdb2, err = orbitdb.NewOrbitDB(ctx, ipfs, &orbitdb.NewOrbitDBOptions{Directory: &dbPath2})
		require.NoError(t, err)
		require.NotNil(t, orbitdb2)

		db, err = orbitdb1.Log(ctx, "replication status tests", nil)
		require.NoError(t, err)
		require.NotNil(t, db)

		cleanup := func() {
			dbPath1Clean()
			dbPath2Clean()
			nodeClean()
			orbitdb1.Close()
			orbitdb2.Close()
			db.Close()
		}
		return cleanup
	}

	t.Run("has correct initial state", func(t *testing.T) {
		defer setup(t)()
		require.Equal(t, db.ReplicationStatus().GetProgress(), 0)
		require.Equal(t, db.ReplicationStatus().GetMax(), 0)
	})

	t.Run("has correct replication info after load", func(t *testing.T) {
		subSetup := func(t *testing.T) func() {
			mainSetupCleanup := setup(t)

			_, err := db.Add(ctx, []byte("hello"))
			require.NoError(t, err)

			require.Nil(t, db.Close())

			db, err = orbitdb1.Log(ctx, "replication status tests", nil)
			require.NoError(t, err)

			require.Nil(t, db.Load(ctx, -1)) // infinity
			require.Equal(t, 1, db.ReplicationStatus().GetProgress())
			require.Equal(t, 1, db.ReplicationStatus().GetMax())

			cleanup := func() {
				db.Close()
				mainSetupCleanup()
			}
			return cleanup
		}

		t.Run("has correct replication info after close", func(t *testing.T) {
			defer subSetup(t)()
			require.Nil(t, db.Close())
			require.Equal(t, 0, db.ReplicationStatus().GetProgress())
			require.Equal(t, 0, db.ReplicationStatus().GetMax())
		})

		t.Run("has correct replication info after sync", func(t *testing.T) {
			defer subSetup(t)()
			_, err := db.Add(ctx, []byte("hello2"))
			require.NoError(t, err)

			require.Equal(t, db.ReplicationStatus().GetProgress(), 2)
			require.Equal(t, db.ReplicationStatus().GetMax(), 2)

			create := false
			db2, err = orbitdb2.Log(ctx, db.Address().String(), &orbitdb.CreateDBOptions{Create: &create})
			require.NoError(t, err)

			sub, err := db2.EventBus().Subscribe(new(stores.EventReplicated))
			require.NoError(t, err)
			defer sub.Close()

			err = db2.Sync(ctx, db.OpLog().Heads().Slice())
			require.NoError(t, err)

			evt := <-sub.Out()
			require.NotNil(t, evt)

			assert.Equal(t, db2.ReplicationStatus().GetProgress(), 2)
			assert.Equal(t, db2.ReplicationStatus().GetMax(), 2)
		})

		t.Run("has correct replication info after loading from snapshot", func(t *testing.T) {
			t.Skip("too fast for a snapshot")
			defer subSetup(t)()
			_, err := db.Add(ctx, []byte("hello2"))
			require.NoError(t, err)

			_, err = basestore.SaveSnapshot(ctx, db)
			require.NoError(t, err)

			db, err = orbitdb1.Log(ctx, "replication status tests", nil)
			require.NoError(t, err)

			err = db.LoadFromSnapshot(ctx)
			require.NoError(t, err)

			<-time.After(100 * time.Millisecond)

			require.Equal(t, db.ReplicationStatus().GetProgress(), 2)
			require.Equal(t, db.ReplicationStatus().GetMax(), 2)
		})
	})
}
