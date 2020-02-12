package tests

import (
	"context"
	"testing"
	"time"

	orbitdb "berty.tech/go-orbit-db"

	"github.com/stretchr/testify/assert"
)

func TestReplicationStatus(t *testing.T) {
	t.Run("orbit-db - Replication Status", func(t *testing.T) {
		var db, db2 orbitdb.EventLogStore

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
		defer cancel()

		infinity := -1
		create := false
		dbPath1, clean := testingTempDir(t, "db1")
		defer clean()

		dbPath2, clean := testingTempDir(t, "db2")
		defer clean()

		mocknet := testingMockNet(ctx)
		node, clean := testingIPFSNode(ctx, t, mocknet)
		defer clean()

		ipfs := testingCoreAPI(t, node)

		orbitdb1, err := orbitdb.NewOrbitDB(ctx, ipfs, &orbitdb.NewOrbitDBOptions{Directory: &dbPath1})
		assert.NoError(t, err)

		orbitdb2, err := orbitdb.NewOrbitDB(ctx, ipfs, &orbitdb.NewOrbitDBOptions{Directory: &dbPath2})
		assert.NoError(t, err)

		db, err = orbitdb1.Log(ctx, "replication status tests", nil)
		assert.NoError(t, err)

		t.Run("has correct initial state", func(t *testing.T) {
			assert.Equal(t, 0, db.ReplicationStatus().GetBuffered())
			assert.Equal(t, 0, db.ReplicationStatus().GetQueued())
			assert.Equal(t, 0, db.ReplicationStatus().GetProgress())
			assert.Equal(t, 0, db.ReplicationStatus().GetMax())
		})

		t.Run("has correct replication info after load", func(t *testing.T) {
			_, err = db.Add(ctx, []byte("hello"))
			assert.NoError(t, err)

			assert.Nil(t, db.Close())

			db, err = orbitdb1.Log(ctx, "replication status tests", nil)
			assert.NoError(t, err)

			assert.Nil(t, db.Load(ctx, infinity))
			assert.Equal(t, 0, db.ReplicationStatus().GetBuffered())
			assert.Equal(t, 0, db.ReplicationStatus().GetQueued())
			assert.Equal(t, 1, db.ReplicationStatus().GetProgress())
			assert.Equal(t, 1, db.ReplicationStatus().GetMax())

			t.Run("has correct replication info after close", func(t *testing.T) {
				assert.Nil(t, db.Close())
				assert.Equal(t, 0, db.ReplicationStatus().GetBuffered())
				assert.Equal(t, 0, db.ReplicationStatus().GetQueued())
				assert.Equal(t, 0, db.ReplicationStatus().GetProgress())
				assert.Equal(t, 0, db.ReplicationStatus().GetMax())
			})

			t.Run("has correct replication info after sync", func(t *testing.T) {
				_, err = db.Add(ctx, []byte("hello2"))
				assert.NoError(t, err)

				assert.Equal(t, 0, db.ReplicationStatus().GetBuffered())
				assert.Equal(t, 0, db.ReplicationStatus().GetQueued())
				assert.Equal(t, 2, db.ReplicationStatus().GetProgress())
				assert.Equal(t, 2, db.ReplicationStatus().GetMax())

				db2, err = orbitdb2.Log(ctx, db.Address().String(), &orbitdb.CreateDBOptions{Create: &create})
				assert.NoError(t, err)

				err = db2.Sync(ctx, db.OpLog().Heads().Slice())
				assert.NoError(t, err)

				<-time.After(100 * time.Millisecond)

				assert.Equal(t, 0, db2.ReplicationStatus().GetBuffered())
				assert.Equal(t, 0, db2.ReplicationStatus().GetQueued())
				assert.Equal(t, 2, db2.ReplicationStatus().GetProgress())
				assert.Equal(t, 2, db2.ReplicationStatus().GetMax())
			})

			//t.Run("has correct replication info after loading from snapshot", func(t *testing.T) {
			//	_, err = db.SaveSnapshot(ctx)
			//	assert.NoError(t, err)
			//
			//	db, err = orbitdb1.Log(ctx, "replication status tests", nil)
			//	assert.NoError(t, err)
			//
			//	err = db.LoadFromSnapshot(ctx)
			//	assert.NoError(t, err)
			//
			//	assert.Equal(t,  0, db.ReplicationStatus().GetBuffered())
			//	assert.Equal(t,  0, db.ReplicationStatus().GetQueued())
			//	assert.Equal(t,  2, db.ReplicationStatus().GetProgress())
			//	assert.Equal(t,  2, db.ReplicationStatus().GetMax())
			//})
		})

		if orbitdb1 != nil {
			err = orbitdb1.Close()
			assert.NoError(t, err)
		}

		if orbitdb2 != nil {
			err = orbitdb2.Close()
			assert.NoError(t, err)
		}
	})
}
