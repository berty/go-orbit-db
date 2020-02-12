package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/stores"
	"berty.tech/go-orbit-db/stores/operation"

	"github.com/stretchr/testify/assert"
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

	t.Run("orbit-db - Create & Open", func(t *testing.T) {
		db1Path, clean := testingTempDir(t, "db1")
		defer clean()

		orbitdb1, err := orbitdb.NewOrbitDB(ctx, db1IPFS, &orbitdb.NewOrbitDBOptions{
			Directory: &db1Path,
		})

		assert.NoError(t, err)

		t.Run("load", func(t *testing.T) {
			dbName := fmt.Sprintf("%d", time.Now().UnixNano())

			db, err := orbitdb1.Log(ctx, dbName, nil)
			assert.NoError(t, err)
			address := db.Address()

			defer db.Drop()
			for i := 0; i < entryCount; i++ {
				_, err := db.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
				assert.NoError(t, err)
			}

			t.Run("loads database from local cache", func(t *testing.T) {
				db, err := orbitdb1.Log(ctx, address.String(), nil)
				assert.NoError(t, err)

				err = db.Load(ctx, infinity)
				assert.NoError(t, err)

				items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
				assert.Equal(t, entryCount, len(items))
				assert.Equal(t, "hello0", string(items[0].GetValue()))
				assert.Equal(t, fmt.Sprintf("hello%d", entryCount-1), string(items[len(items)-1].GetValue()))
			})

			t.Run("loads database partially", func(t *testing.T) {
				amount := 33
				db, err := orbitdb1.Log(ctx, address.String(), nil)
				assert.NoError(t, err)

				err = db.Load(ctx, amount)
				assert.NoError(t, err)

				items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
				assert.NoError(t, err)

				assert.Equal(t, amount, len(items))
				assert.Equal(t, fmt.Sprintf("hello%d", entryCount-amount), string(items[0].GetValue()))
				assert.Equal(t, fmt.Sprintf("hello%d", entryCount-amount+1), string(items[1].GetValue()))
				assert.Equal(t, fmt.Sprintf("hello%d", entryCount-1), string(items[len(items)-1].GetValue()))
			})

			t.Run("load and close several times", func(t *testing.T) {
				amount := 8
				for i := 0; i < amount; i++ {
					db, err := orbitdb1.Log(ctx, address.String(), nil)
					assert.NoError(t, err)

					err = db.Load(ctx, infinity)
					assert.NoError(t, err)

					items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					assert.NoError(t, err)

					assert.Equal(t, entryCount, len(items))
					assert.Equal(t, "hello0", string(items[0].GetValue()))
					assert.Equal(t, "hello1", string(items[1].GetValue()))
					assert.Equal(t, fmt.Sprintf("hello%d", entryCount-1), string(items[len(items)-1].GetValue()))

					err = db.Close()
					assert.NoError(t, err)
				}
			})

			t.Run("closes database while loading", func(t *testing.T) {
				db, err := orbitdb1.Log(ctx, address.String(), nil)
				assert.NoError(t, err)

				err = db.Load(ctx, -1) // don't wait for load to finish
				assert.NoError(t, err)

				err = db.Close()
				assert.NoError(t, err)

				//TODO: assert.equal(db._cache.store, null)
			})

			t.Run("load, add one, close - several times", func(t *testing.T) {
				const amount = 8
				for i := 0; i < amount; i++ {
					db, err := orbitdb1.Log(ctx, address.String(), nil)
					assert.NoError(t, err)

					err = db.Load(ctx, infinity)
					assert.NoError(t, err)

					_, err = db.Add(ctx, []byte(fmt.Sprintf("hello%d", entryCount+i)))
					assert.NoError(t, err)

					items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					assert.NoError(t, err)

					assert.Equal(t, entryCount+i+1, len(items))
					assert.Equal(t, fmt.Sprintf("hello%d", entryCount+i), string(items[len(items)-1].GetValue()))

					err = db.Close()
					assert.NoError(t, err)
				}
			})

			t.Run("loading a database emits 'ready' event", func(t *testing.T) {
				db, err := orbitdb1.Log(ctx, address.String(), nil)
				assert.NoError(t, err)

				wg := sync.WaitGroup{}
				wg.Add(1)
				l := sync.RWMutex{}

				var items []operation.Operation

				go db.Subscribe(ctx, func(evt events.Event) {
					switch evt.(type) {
					case *stores.EventReady:
						l.Lock()
						items, err = db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
						l.Unlock()
						wg.Done()
						return
					}
				})

				assert.Nil(t, db.Load(ctx, infinity))
				wg.Wait()

				l.RLock()
				assert.Equal(t, entryCount, len(items))
				assert.Equal(t, "hello0", string(items[0].GetValue()))
				assert.Equal(t, fmt.Sprintf("hello%d", entryCount-1), string(items[len(items)-1].GetValue()))
				l.RUnlock()
			})

			t.Run("loading a database emits 'load.progress' event", func(t *testing.T) {
				// TODO:
			})

			t.Run("load from empty snapshot", func(t *testing.T) {
				t.Run("loads database from an empty snapshot", func(t *testing.T) {
					db, err := orbitdb1.Log(ctx, "empty-snapshot", nil)
					assert.NoError(t, err)

					address := db.Address().String()
					_, err = db.SaveSnapshot(ctx)
					assert.NoError(t, err)

					err = db.Close()
					assert.NoError(t, err)

					dbUntyped, err := orbitdb1.Open(ctx, address, nil)
					assert.NoError(t, err)
					db, ok := dbUntyped.(orbitdb.EventLogStore)
					assert.True(t, ok)

					err = db.LoadFromSnapshot(ctx)
					assert.NoError(t, err)

					items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					assert.NoError(t, err)
					assert.Equal(t, 0, len(items))
				})
			})

			t.Run("load from snapshot", func(t *testing.T) {
				dbName := time.Now().String()
				var entryArr []operation.Operation

				db, err := orbitdb1.Log(ctx, dbName, nil)
				assert.NoError(t, err)

				address := db.Address().String()

				for i := 0; i < entryCount; i++ {
					op, err := db.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
					assert.NoError(t, err)

					entryArr = append(entryArr, op)
				}

				_, err = db.SaveSnapshot(ctx)
				assert.NoError(t, err)

				err = db.Close()
				assert.NoError(t, err)
				db = nil

				t.Run("loads database from snapshot", func(t *testing.T) {
					db, err = orbitdb1.Log(ctx, address, nil)
					assert.NoError(t, err)

					err = db.LoadFromSnapshot(ctx)
					assert.NoError(t, err)

					items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					assert.NoError(t, err)

					assert.Equal(t, entryCount, len(items))
					assert.Equal(t, "hello0", string(items[0].GetValue()))
					assert.Equal(t, fmt.Sprintf("hello%d", entryCount-1), string(items[entryCount-1].GetValue()))
				})

				t.Run("load, add one and save snapshot several times", func(t *testing.T) {
					const amount = 4

					for i := 0; i < amount; i++ {
						db, err := orbitdb1.Log(ctx, address, nil)
						assert.NoError(t, err)

						err = db.LoadFromSnapshot(ctx)
						assert.NoError(t, err)

						_, err = db.Add(ctx, []byte(fmt.Sprintf("hello%d", entryCount+i)))
						assert.NoError(t, err)

						items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
						assert.NoError(t, err)

						assert.Equal(t, entryCount+i+1, len(items))
						assert.Equal(t, "hello0", string(items[0].GetValue()))
						assert.Equal(t, fmt.Sprintf("hello%d", entryCount+i), string(items[len(items)-1].GetValue()))

						_, err = db.SaveSnapshot(ctx)
						assert.NoError(t, err)

						err = db.Close()
						assert.NoError(t, err)
					}
				})

				t.Run("throws an error when trying to load a missing snapshot", func(t *testing.T) {
					db, err := orbitdb1.Log(ctx, address, nil)
					assert.NoError(t, err)

					err = db.Drop()
					assert.NoError(t, err)

					db, err = orbitdb1.Log(ctx, address, nil)
					assert.NoError(t, err)

					err = db.LoadFromSnapshot(ctx)
					assert.NotNil(t, err)
					assert.Contains(t, err.Error(), "not found")
				})

				t.Run("loading a database emits 'ready' event", func(t *testing.T) {
					// TODO
				})

				t.Run("loading a database emits 'load.progress' event", func(t *testing.T) {
					// TODO
				})

				if db != nil {
					err = db.Drop()
					assert.NoError(t, err)
				}
			})
		})
	})
}
