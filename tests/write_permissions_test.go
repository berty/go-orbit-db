package tests

import (
	"context"

	"berty.tech/go-orbit-db/accesscontroller"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/stores"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"testing"
	"time"
)

func TestWritePermissions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	mocknet := testingMockNet(ctx)

	node, clean := testingIPFSNode(ctx, t, mocknet)
	defer clean()

	ipfs := testingCoreAPI(t, node)

	Convey("orbit-db - Write Permissions", t, FailureHalts, func(c C) {
		dbPath1, clean := testingTempDir(t, "db1")
		defer clean()

		dbPath2, clean := testingTempDir(t, "db2")
		defer clean()

		orbitdb1, err := orbitdb.NewOrbitDB(ctx, ipfs, &orbitdb.NewOrbitDBOptions{Directory: &dbPath1})
		assert.NoError(t, err)
		defer orbitdb1.Close()

		orbitdb2, err := orbitdb.NewOrbitDB(ctx, ipfs, &orbitdb.NewOrbitDBOptions{Directory: &dbPath2})
		assert.NoError(t, err)
		defer orbitdb2.Close()

		c.Convey("allows multiple peers to write to the databases", FailureHalts, func(c C) {
			c.Convey("eventlog allows multiple writers", FailureHalts, func(c C) {
				ac := &accesscontroller.CreateAccessControllerOptions{
					Access: map[string][]string{
						"write": {
							orbitdb1.Identity().ID,
							orbitdb2.Identity().ID,
						},
					},
				}

				db1, err := orbitdb1.Log(ctx, "sync-test", &orbitdb.CreateDBOptions{
					AccessController: ac,
				})
				assert.NoError(t, err)
				defer db1.Close()

				db2, err := orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
					AccessController: ac,
				})
				assert.NoError(t, err)
				defer db2.Close()

				_, err = db1.Add(ctx, []byte("hello"))
				assert.NoError(t, err)

				_, err = db2.Add(ctx, []byte("hello"))
				assert.NoError(t, err)

				values, err := db1.List(ctx, nil)
				assert.NoError(t, err)
				assert.Equal(t, 1, len(values))

				assert.Equal(t, "hello", string(values[0].GetValue()))

				values, err = db2.List(ctx, nil)
				assert.NoError(t, err)
				assert.Equal(t, 1, len(values))

				assert.Equal(t, "hello", string(values[0].GetValue()))
			})
		})

		c.Convey("syncs databases", FailureHalts, func(c C) {
			c.Convey("eventlog syncs", FailureHalts, func(c C) {
				ac := &accesscontroller.CreateAccessControllerOptions{
					Access: map[string][]string{
						"write": {
							orbitdb1.Identity().ID,
							orbitdb2.Identity().ID,
						},
					},
				}

				db1, err := orbitdb1.Log(ctx, "sync-test", &orbitdb.CreateDBOptions{
					AccessController: ac,
				})
				assert.NoError(t, err)
				defer db1.Close()

				db2, err := orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
					AccessController: ac,
				})
				assert.NoError(t, err)
				defer db2.Close()

				_, err = db2.Add(ctx, []byte("hello"))
				assert.NoError(t, err)

				assert.Equal(t, 0, db1.OpLog().Values().Len())

				err = db1.Sync(ctx, db2.OpLog().Heads().Slice())
				assert.NoError(t, err)

				<-time.After(time.Millisecond * 300)

				values, err := db1.List(ctx, nil)
				assert.NoError(t, err)
				assert.Equal(t, 1, len(values))

				assert.Equal(t, "hello", string(values[0].GetValue()))
			})
		})

		c.Convey("syncs databases that anyone can write to", FailureHalts, func(c C) {
			c.Convey("eventlog syncs", FailureHalts, func(c C) {
				ac := &accesscontroller.CreateAccessControllerOptions{
					Access: map[string][]string{
						"write": {
							"*",
						},
					},
				}

				db1, err := orbitdb1.Log(ctx, "sync-test-public-dbs", &orbitdb.CreateDBOptions{
					AccessController: ac,
				})
				assert.NoError(t, err)
				defer db1.Close()

				db2, err := orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
					AccessController: ac,
				})
				assert.NoError(t, err)
				defer db2.Close()

				_, err = db2.Add(ctx, []byte("hello"))
				assert.NoError(t, err)

				assert.Equal(t, 0, db1.OpLog().Values().Len())

				err = db1.Sync(ctx, db2.OpLog().Heads().Slice())
				assert.NoError(t, err)

				<-time.After(time.Millisecond * 300)

				values, err := db1.List(ctx, nil)
				assert.NoError(t, err)
				assert.Equal(t, 1, len(values))

				assert.Equal(t, "hello", string(values[0].GetValue()))
			})
		})

		c.Convey("doesn't sync if peer is not allowed to write to the database", FailureHalts, func(c C) {
			c.Convey("eventlog doesn't sync", FailureHalts, func(c C) {
				ac := &accesscontroller.CreateAccessControllerOptions{
					Access: map[string][]string{
						"write": {orbitdb1.Identity().ID},
					},
				}

				db1, err := orbitdb1.Log(ctx, "write error test 1", &orbitdb.CreateDBOptions{
					AccessController: ac,
				})
				assert.NoError(t, err)
				defer db1.Close()

				db2, err := orbitdb2.Log(ctx, "write error test 1", &orbitdb.CreateDBOptions{
					AccessController: ac,
				})
				assert.NoError(t, err)
				defer db2.Close()

				subCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()

				go db1.Subscribe(subCtx, func(evt events.Event) {
					switch evt.(type) {
					case *stores.EventReplicated:
						assert.Equal(t, "should not occur", "this")
					}
				})

				_, err = db2.Add(ctx, []byte("hello"))
				assert.NotNil(t, err)

				<-subCtx.Done()

				err = db1.Sync(ctx, db2.OpLog().Heads().Slice())
				assert.NoError(t, err)

				<-time.After(300 * time.Millisecond)

				assert.Equal(t, 0, db1.OpLog().Values().Len())
				assert.Equal(t, 0, db2.OpLog().Values().Len())

				_, err = db1.Add(ctx, []byte("hello"))
				assert.NoError(t, err)

				<-subCtx.Done()

				err = db2.Sync(ctx, db1.OpLog().Heads().Slice())
				assert.NoError(t, err)

				<-time.After(300 * time.Millisecond)

				assert.Equal(t, 1, db1.OpLog().Values().Len())
				assert.Equal(t, 1, db2.OpLog().Values().Len())
			})
		})

		c.Convey("throws an error if peer is not allowed to write to the database", FailureHalts, func(c C) {
			c.Convey("eventlog doesn't sync", FailureHalts, func(c C) {
				ac := &accesscontroller.CreateAccessControllerOptions{
					Access: map[string][]string{
						"write": {},
					},
				}

				db1, err := orbitdb1.Log(ctx, "write error test 2", &orbitdb.CreateDBOptions{
					AccessController: ac,
				})
				assert.NoError(t, err)
				defer db1.Close()

				db2, err := orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
					AccessController: ac,
				})
				assert.NoError(t, err)
				defer db2.Close()

				_, err = db2.Add(ctx, []byte("hello"))
				assert.NotNil(t, err)
			})
		})
	})
}
