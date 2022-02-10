package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/address"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores"
	"berty.tech/go-orbit-db/stores/basestore"
	"berty.tech/go-orbit-db/stores/operation"
	"github.com/stretchr/testify/require"
)

func TestPersistence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entryCount := 65
	infinity := -1

	mocknet := testingMockNet(ctx)
	node, clean := testingIPFSNode(ctx, t, mocknet)
	defer clean()

	db1IPFS := testingCoreAPI(t, node)

	// setup
	var (
		orbitdb1 iface.OrbitDB
		address  address.Address
		db       orbitdb.EventLogStore
	)
	setup := func(t *testing.T) func() {
		db1Path, db1PathClean := testingTempDir(t, "db1")

		var err error
		orbitdb1, err = orbitdb.NewOrbitDB(ctx, db1IPFS, &orbitdb.NewOrbitDBOptions{
			Directory: &db1Path,
		})

		require.NoError(t, err)

		dbName := fmt.Sprintf("%d", time.Now().UnixNano())

		db, err = orbitdb1.Log(ctx, dbName, nil)
		require.NoError(t, err)
		address = db.Address()

		for i := 0; i < entryCount; i++ {
			_, err := db.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
			require.NoError(t, err)
		}

		cleanup := func() {
			db.Close()
			db.Drop()
			orbitdb1.Close()
			db1PathClean()
		}
		return cleanup
	}

	t.Run("loads database from local cache", func(t *testing.T) {
		defer setup(t)()
		db, err := orbitdb1.Log(ctx, address.String(), nil)
		require.NoError(t, err)

		err = db.Load(ctx, infinity)
		require.NoError(t, err)

		items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
		require.NoError(t, err)

		require.Equal(t, len(items), entryCount)
		require.Equal(t, string(items[0].GetValue()), "hello0")
		require.Equal(t, string(items[len(items)-1].GetValue()), fmt.Sprintf("hello%d", entryCount-1))
	})

	t.Run("loads database partially", func(t *testing.T) {
		defer setup(t)()
		amount := 33
		db, err := orbitdb1.Log(ctx, address.String(), nil)
		require.NoError(t, err)

		err = db.Load(ctx, amount)
		require.NoError(t, err)

		items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
		require.NoError(t, err)

		require.Equal(t, len(items), amount)
		require.Equal(t, string(items[0].GetValue()), fmt.Sprintf("hello%d", entryCount-amount))
		require.Equal(t, string(items[1].GetValue()), fmt.Sprintf("hello%d", entryCount-amount+1))
		require.Equal(t, string(items[len(items)-1].GetValue()), fmt.Sprintf("hello%d", entryCount-1))
	})

	t.Run("load and close several times", func(t *testing.T) {
		defer setup(t)()
		amount := 8
		for i := 0; i < amount; i++ {
			db, err := orbitdb1.Log(ctx, address.String(), nil)
			require.NoError(t, err)

			err = db.Load(ctx, infinity)
			require.NoError(t, err)

			items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
			require.NoError(t, err)

			require.Equal(t, len(items), entryCount)
			require.Equal(t, string(items[0].GetValue()), "hello0")
			require.Equal(t, string(items[1].GetValue()), "hello1")
			require.Equal(t, string(items[len(items)-1].GetValue()), fmt.Sprintf("hello%d", entryCount-1))

			err = db.Close()
			require.NoError(t, err)
		}
	})

	t.Run("closes database while loading", func(t *testing.T) {
		defer setup(t)()
		db, err := orbitdb1.Log(ctx, address.String(), nil)
		require.NoError(t, err)

		err = db.Load(ctx, -1) // don't wait for load to finish
		require.NoError(t, err)

		err = db.Close()
		require.NoError(t, err)

		// TODO: assert.equal(db._cache.store, null)
	})

	t.Run("load, add one, close - several times", func(t *testing.T) {
		defer setup(t)()
		const amount = 8
		for i := 0; i < amount; i++ {
			db, err := orbitdb1.Log(ctx, address.String(), nil)
			require.NoError(t, err)

			err = db.Load(ctx, infinity)
			require.NoError(t, err)

			_, err = db.Add(ctx, []byte(fmt.Sprintf("hello%d", entryCount+i)))
			require.NoError(t, err)

			items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
			require.NoError(t, err)

			require.Equal(t, len(items), entryCount+i+1)
			require.Equal(t, string(items[len(items)-1].GetValue()), fmt.Sprintf("hello%d", entryCount+i))

			err = db.Close()
			require.NoError(t, err)
		}
	})

	t.Run("loading a database emits 'ready' event", func(t *testing.T) {
		defer setup(t)()
		db, err := orbitdb1.Log(ctx, address.String(), nil)
		require.NoError(t, err)

		l := sync.RWMutex{}

		var items []operation.Operation

		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()

		sub, err := db.EventBus().Subscribe(new(stores.EventReady))
		require.NoError(t, err)

		defer sub.Close()

		go func() {
			for evt := range sub.Out() {
				switch evt.(type) {
				case stores.EventReady:
					l.Lock()
					items, err = db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					l.Unlock()
					cancel()
					continue
				}
			}
		}()

		require.Nil(t, db.Load(ctx, infinity))
		<-ctx.Done()

		l.RLock()
		require.Equal(t, len(items), entryCount)
		require.Equal(t, string(items[0].GetValue()), "hello0")
		require.Equal(t, string(items[len(items)-1].GetValue()), fmt.Sprintf("hello%d", entryCount-1))
		l.RUnlock()
	})

	t.Run("loading a database emits 'load.progress' event", func(t *testing.T) {
		// TODO:
	})

	t.Run("load from empty snapshot", func(t *testing.T) {
		t.Run("loads database from an empty snapshot", func(t *testing.T) {
			defer setup(t)()
			db, err := orbitdb1.Log(ctx, "empty-snapshot", nil)
			require.NoError(t, err)

			address := db.Address()
			_, err = basestore.SaveSnapshot(ctx, db)
			require.NoError(t, err)

			err = db.Close()
			require.NoError(t, err)

			dbUntyped, err := orbitdb1.Open(ctx, address.String(), nil)
			require.NoError(t, err)
			db, ok := dbUntyped.(orbitdb.EventLogStore)
			require.True(t, ok)

			err = db.LoadFromSnapshot(ctx)
			require.NoError(t, err)

			items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
			require.NoError(t, err)
			require.Equal(t, len(items), 0)
		})
	})

	t.Run("load from snapshot", func(t *testing.T) {
		// setup
		subSetup := func(t *testing.T) func() {
			setupCleanup := setup(t)

			dbName := fmt.Sprintf("%d", time.Now().UnixNano())

			db, err := orbitdb1.Log(ctx, dbName, nil)
			require.NoError(t, err)

			address = db.Address()

			for i := 0; i < entryCount; i++ {
				_, err := db.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
				require.NoError(t, err)
			}

			_, err = basestore.SaveSnapshot(ctx, db)
			require.NoError(t, err)

			err = db.Close()
			require.NoError(t, err)
			db = nil

			cleanup := func() {
				setupCleanup()
			}
			return cleanup
		}

		t.Run("loads database from snapshot", func(t *testing.T) {
			defer subSetup(t)()
			var err error
			db, err = orbitdb1.Log(ctx, address.String(), nil)
			require.NoError(t, err)

			err = db.LoadFromSnapshot(ctx)
			require.NoError(t, err)

			items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
			require.NoError(t, err)

			require.Equal(t, len(items), entryCount)
			require.Equal(t, string(items[0].GetValue()), "hello0")
			require.Equal(t, string(items[entryCount-1].GetValue()), fmt.Sprintf("hello%d", entryCount-1))

			db.Drop()
			db.Close()
		})

		t.Run("load, add one and save snapshot several times", func(t *testing.T) {
			defer subSetup(t)()
			const amount = 4

			for i := 0; i < amount; i++ {
				db, err := orbitdb1.Log(ctx, address.String(), nil)
				require.NoError(t, err)

				err = db.LoadFromSnapshot(ctx)
				require.NoError(t, err)

				_, err = db.Add(ctx, []byte(fmt.Sprintf("hello%d", entryCount+i)))
				require.NoError(t, err)

				items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
				require.NoError(t, err)

				require.Equal(t, len(items), entryCount+i+1)
				require.Equal(t, string(items[0].GetValue()), "hello0")
				require.Equal(t, string(items[len(items)-1].GetValue()), fmt.Sprintf("hello%d", entryCount+i))

				_, err = basestore.SaveSnapshot(ctx, db)
				require.NoError(t, err)

				err = db.Close()
				require.NoError(t, err)
			}
		})

		t.Run("throws an error when trying to load a missing snapshot", func(t *testing.T) {
			defer subSetup(t)()
			db, err := orbitdb1.Log(ctx, address.String(), nil)
			require.NoError(t, err)

			err = db.Drop()
			require.NoError(t, err)

			db, err = orbitdb1.Log(ctx, address.String(), nil)
			require.NoError(t, err)

			err = db.LoadFromSnapshot(ctx)
			require.Error(t, err)
			require.Contains(t, err.Error(), "not found")
		})

		t.Run("loading a database emits 'ready' event", func(t *testing.T) {
			// TODO
		})

		t.Run("loading a database emits 'load.progress' event", func(t *testing.T) {
			// TODO
		})
	})
}
