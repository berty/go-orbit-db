package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/stores"
	"berty.tech/go-orbit-db/stores/basestore"
	"berty.tech/go-orbit-db/stores/operation"
	. "github.com/smartystreets/goconvey/convey"
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

	Convey("orbit-db - Create & Open", t, FailureHalts, func(c C) {
		db1Path, clean := testingTempDir(t, "db1")
		defer clean()

		orbitdb1, err := orbitdb.NewOrbitDB(ctx, db1IPFS, &orbitdb.NewOrbitDBOptions{
			Directory: &db1Path,
		})

		c.So(err, ShouldBeNil)

		defer orbitdb1.Close()

		c.Convey("load", FailureHalts, func(c C) {
			dbName := fmt.Sprintf("%d", time.Now().UnixNano())

			db, err := orbitdb1.Log(ctx, dbName, nil)
			c.So(err, ShouldBeNil)
			address := db.Address()

			defer db.Drop()
			defer db.Close()

			for i := 0; i < entryCount; i++ {
				_, err := db.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
				c.So(err, ShouldBeNil)
			}

			c.Convey("loads database from local cache", FailureHalts, func(c C) {
				db, err := orbitdb1.Log(ctx, address.String(), nil)
				c.So(err, ShouldBeNil)

				err = db.Load(ctx, infinity)
				c.So(err, ShouldBeNil)

				items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
				c.So(len(items), ShouldEqual, entryCount)
				c.So(string(items[0].GetValue()), ShouldEqual, "hello0")
				c.So(string(items[len(items)-1].GetValue()), ShouldEqual, fmt.Sprintf("hello%d", entryCount-1))
			})

			c.Convey("loads database partially", FailureHalts, func(c C) {
				amount := 33
				db, err := orbitdb1.Log(ctx, address.String(), nil)
				c.So(err, ShouldBeNil)

				err = db.Load(ctx, amount)
				c.So(err, ShouldBeNil)

				items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
				c.So(err, ShouldBeNil)

				c.So(len(items), ShouldEqual, amount)
				c.So(string(items[0].GetValue()), ShouldEqual, fmt.Sprintf("hello%d", entryCount-amount))
				c.So(string(items[1].GetValue()), ShouldEqual, fmt.Sprintf("hello%d", entryCount-amount+1))
				c.So(string(items[len(items)-1].GetValue()), ShouldEqual, fmt.Sprintf("hello%d", entryCount-1))
			})

			c.Convey("load and close several times", FailureHalts, func(c C) {
				amount := 8
				for i := 0; i < amount; i++ {
					db, err := orbitdb1.Log(ctx, address.String(), nil)
					c.So(err, ShouldBeNil)

					err = db.Load(ctx, infinity)
					c.So(err, ShouldBeNil)

					items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					c.So(err, ShouldBeNil)

					c.So(len(items), ShouldEqual, entryCount)
					c.So(string(items[0].GetValue()), ShouldEqual, "hello0")
					c.So(string(items[1].GetValue()), ShouldEqual, "hello1")
					c.So(string(items[len(items)-1].GetValue()), ShouldEqual, fmt.Sprintf("hello%d", entryCount-1))

					err = db.Close()
					c.So(err, ShouldBeNil)
				}
			})

			c.Convey("closes database while loading", FailureHalts, func(c C) {
				db, err := orbitdb1.Log(ctx, address.String(), nil)
				c.So(err, ShouldBeNil)

				err = db.Load(ctx, -1) // don't wait for load to finish
				c.So(err, ShouldBeNil)

				err = db.Close()
				c.So(err, ShouldBeNil)

				// TODO: assert.equal(db._cache.store, null)
			})

			c.Convey("load, add one, close - several times", FailureHalts, func(c C) {
				const amount = 8
				for i := 0; i < amount; i++ {
					db, err := orbitdb1.Log(ctx, address.String(), nil)
					c.So(err, ShouldBeNil)

					err = db.Load(ctx, infinity)
					c.So(err, ShouldBeNil)

					_, err = db.Add(ctx, []byte(fmt.Sprintf("hello%d", entryCount+i)))
					c.So(err, ShouldBeNil)

					items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					c.So(err, ShouldBeNil)

					c.So(len(items), ShouldEqual, entryCount+i+1)
					c.So(string(items[len(items)-1].GetValue()), ShouldEqual, fmt.Sprintf("hello%d", entryCount+i))

					err = db.Close()
					c.So(err, ShouldBeNil)
				}
			})

			c.Convey("loading a database emits 'ready' event", FailureHalts, func(c C) {
				db, err := orbitdb1.Log(ctx, address.String(), nil)
				c.So(err, ShouldBeNil)

				l := sync.RWMutex{}

				var items []operation.Operation

				ctx, cancel := context.WithTimeout(ctx, time.Second*5)
				defer cancel()

				go func() {
					for evt := range db.Subscribe(ctx) {
						switch evt.(type) {
						case *stores.EventReady:
							l.Lock()
							items, err = db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
							l.Unlock()
							cancel()
							continue
						}
					}
				}()

				c.So(db.Load(ctx, infinity), ShouldBeNil)
				<-ctx.Done()

				l.RLock()
				c.So(len(items), ShouldEqual, entryCount)
				c.So(string(items[0].GetValue()), ShouldEqual, "hello0")
				c.So(string(items[len(items)-1].GetValue()), ShouldEqual, fmt.Sprintf("hello%d", entryCount-1))
				l.RUnlock()
			})

			c.Convey("loading a database emits 'load.progress' event", FailureHalts, func(c C) {
				// TODO:
			})

			c.Convey("load from empty snapshot", FailureHalts, func(c C) {
				c.Convey("loads database from an empty snapshot", FailureHalts, func(c C) {
					db, err := orbitdb1.Log(ctx, "empty-snapshot", nil)
					c.So(err, ShouldBeNil)

					address := db.Address().String()
					_, err = basestore.SaveSnapshot(ctx, db)
					c.So(err, ShouldBeNil)

					err = db.Close()
					c.So(err, ShouldBeNil)

					dbUntyped, err := orbitdb1.Open(ctx, address, nil)
					c.So(err, ShouldBeNil)
					db, ok := dbUntyped.(orbitdb.EventLogStore)
					c.So(ok, ShouldBeTrue)

					err = db.LoadFromSnapshot(ctx)
					c.So(err, ShouldBeNil)

					items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					c.So(err, ShouldBeNil)
					c.So(len(items), ShouldEqual, 0)
				})
			})

			c.Convey("load from snapshot", FailureHalts, func(c C) {
				dbName := fmt.Sprintf("%d", time.Now().UnixNano())
				var entryArr []operation.Operation

				db, err := orbitdb1.Log(ctx, dbName, nil)
				c.So(err, ShouldBeNil)

				address := db.Address().String()

				for i := 0; i < entryCount; i++ {
					op, err := db.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
					c.So(err, ShouldBeNil)

					entryArr = append(entryArr, op)
				}

				_, err = basestore.SaveSnapshot(ctx, db)
				c.So(err, ShouldBeNil)

				err = db.Close()
				c.So(err, ShouldBeNil)
				db = nil

				c.Convey("loads database from snapshot", FailureHalts, func(c C) {
					db, err = orbitdb1.Log(ctx, address, nil)
					c.So(err, ShouldBeNil)

					err = db.LoadFromSnapshot(ctx)
					c.So(err, ShouldBeNil)

					items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					c.So(err, ShouldBeNil)

					c.So(len(items), ShouldEqual, entryCount)
					c.So(string(items[0].GetValue()), ShouldEqual, "hello0")
					c.So(string(items[entryCount-1].GetValue()), ShouldEqual, fmt.Sprintf("hello%d", entryCount-1))

					db.Drop()
					db.Close()
				})

				c.Convey("load, add one and save snapshot several times", FailureHalts, func(c C) {
					const amount = 4

					for i := 0; i < amount; i++ {
						db, err := orbitdb1.Log(ctx, address, nil)
						c.So(err, ShouldBeNil)

						err = db.LoadFromSnapshot(ctx)
						c.So(err, ShouldBeNil)

						_, err = db.Add(ctx, []byte(fmt.Sprintf("hello%d", entryCount+i)))
						c.So(err, ShouldBeNil)

						items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
						c.So(err, ShouldBeNil)

						c.So(len(items), ShouldEqual, entryCount+i+1)
						c.So(string(items[0].GetValue()), ShouldEqual, "hello0")
						c.So(string(items[len(items)-1].GetValue()), ShouldEqual, fmt.Sprintf("hello%d", entryCount+i))

						_, err = basestore.SaveSnapshot(ctx, db)
						c.So(err, ShouldBeNil)

						err = db.Close()
						c.So(err, ShouldBeNil)
					}
				})

				c.Convey("throws an error when trying to load a missing snapshot", FailureHalts, func(c C) {
					db, err := orbitdb1.Log(ctx, address, nil)
					c.So(err, ShouldBeNil)

					err = db.Drop()
					c.So(err, ShouldBeNil)

					db, err = orbitdb1.Log(ctx, address, nil)
					c.So(err, ShouldBeNil)

					err = db.LoadFromSnapshot(ctx)
					c.So(err, ShouldNotBeNil)
					c.So(err.Error(), ShouldContainSubstring, "not found")
				})

				c.Convey("loading a database emits 'ready' event", FailureHalts, func(c C) {
					// TODO
				})

				c.Convey("loading a database emits 'load.progress' event", FailureHalts, func(c C) {
					// TODO
				})
			})
		})
	})
}
