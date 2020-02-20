package tests

import (
	"context"

	"berty.tech/go-orbit-db/accesscontroller"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/stores"
	. "github.com/smartystreets/goconvey/convey"

	"testing"
	"time"
)

func TestWritePermissions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
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
		c.So(err, ShouldBeNil)
		defer orbitdb1.Close()

		orbitdb2, err := orbitdb.NewOrbitDB(ctx, ipfs, &orbitdb.NewOrbitDBOptions{Directory: &dbPath2})
		c.So(err, ShouldBeNil)
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
				c.So(err, ShouldBeNil)
				defer db1.Close()

				db2, err := orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
					AccessController: ac,
				})
				c.So(err, ShouldBeNil)
				defer db2.Close()

				_, err = db1.Add(ctx, []byte("hello"))
				c.So(err, ShouldBeNil)

				_, err = db2.Add(ctx, []byte("hello"))
				c.So(err, ShouldBeNil)

				values, err := db1.List(ctx, nil)
				c.So(err, ShouldBeNil)
				c.So(len(values), ShouldEqual, 1)

				c.So(string(values[0].GetValue()), ShouldEqual, "hello")

				values, err = db2.List(ctx, nil)
				c.So(err, ShouldBeNil)
				c.So(len(values), ShouldEqual, 1)

				c.So(string(values[0].GetValue()), ShouldEqual, "hello")
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
				c.So(err, ShouldBeNil)
				defer db1.Close()

				db2, err := orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
					AccessController: ac,
				})
				c.So(err, ShouldBeNil)
				defer db2.Close()

				_, err = db2.Add(ctx, []byte("hello"))
				c.So(err, ShouldBeNil)

				c.So(db1.OpLog().Values().Len(), ShouldEqual, 0)

				err = db1.Sync(ctx, db2.OpLog().Heads().Slice())
				c.So(err, ShouldBeNil)

				<-time.After(time.Millisecond * 300)

				values, err := db1.List(ctx, nil)
				c.So(err, ShouldBeNil)
				c.So(len(values), ShouldEqual, 1)

				c.So(string(values[0].GetValue()), ShouldEqual, "hello")
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
				c.So(err, ShouldBeNil)
				defer db1.Close()

				db2, err := orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
					AccessController: ac,
				})
				c.So(err, ShouldBeNil)
				defer db2.Close()

				_, err = db2.Add(ctx, []byte("hello"))
				c.So(err, ShouldBeNil)

				c.So(db1.OpLog().Values().Len(), ShouldEqual, 0)

				err = db1.Sync(ctx, db2.OpLog().Heads().Slice())
				c.So(err, ShouldBeNil)

				<-time.After(time.Millisecond * 300)

				values, err := db1.List(ctx, nil)
				c.So(err, ShouldBeNil)
				c.So(len(values), ShouldEqual, 1)

				c.So(string(values[0].GetValue()), ShouldEqual, "hello")
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
				c.So(err, ShouldBeNil)
				defer db1.Close()

				db2, err := orbitdb2.Log(ctx, "write error test 1", &orbitdb.CreateDBOptions{
					AccessController: ac,
				})
				c.So(err, ShouldBeNil)
				defer db2.Close()

				subCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()

				go db1.Subscribe(subCtx, func(evt events.Event) {
					switch evt.(type) {
					case *stores.EventReplicated:
						c.So("this", ShouldEqual, "should not occur")
					}
				})

				_, err = db2.Add(ctx, []byte("hello"))
				c.So(err, ShouldNotBeNil)

				<-subCtx.Done()

				err = db1.Sync(ctx, db2.OpLog().Heads().Slice())
				c.So(err, ShouldBeNil)

				<-time.After(300 * time.Millisecond)

				c.So(db1.OpLog().Values().Len(), ShouldEqual, 0)
				c.So(db2.OpLog().Values().Len(), ShouldEqual, 0)

				_, err = db1.Add(ctx, []byte("hello"))
				c.So(err, ShouldBeNil)

				<-subCtx.Done()

				err = db2.Sync(ctx, db1.OpLog().Heads().Slice())
				c.So(err, ShouldBeNil)

				<-time.After(300 * time.Millisecond)

				c.So(db1.OpLog().Values().Len(), ShouldEqual, 1)
				c.So(db2.OpLog().Values().Len(), ShouldEqual, 1)
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
				c.So(err, ShouldBeNil)
				defer db1.Close()

				db2, err := orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
					AccessController: ac,
				})
				c.So(err, ShouldBeNil)
				defer db2.Close()

				_, err = db2.Add(ctx, []byte("hello"))
				c.So(err, ShouldNotBeNil)
			})
		})
	})
}
