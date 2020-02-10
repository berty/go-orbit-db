package tests

import (
	"context"
	"testing"
	"time"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/stores/basestore"

	. "github.com/smartystreets/goconvey/convey"
)

func TestReplicationStatus(t *testing.T) {
	Convey("orbit-db - Replication Status", t, FailureHalts, func(c C) {
		var db, db2 orbitdb.EventLogStore

		ctx, cancel := context.WithCancel(context.Background())
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
		c.So(err, ShouldBeNil)

		orbitdb2, err := orbitdb.NewOrbitDB(ctx, ipfs, &orbitdb.NewOrbitDBOptions{Directory: &dbPath2})
		c.So(err, ShouldBeNil)

		db, err = orbitdb1.Log(ctx, "replication status tests", nil)
		c.So(err, ShouldBeNil)

		c.Convey("has correct initial state", FailureHalts, func(c C) {
			c.So(db.ReplicationStatus().GetBuffered(), ShouldEqual, 0)
			c.So(db.ReplicationStatus().GetQueued(), ShouldEqual, 0)
			c.So(db.ReplicationStatus().GetProgress(), ShouldEqual, 0)
			c.So(db.ReplicationStatus().GetMax(), ShouldEqual, 0)
		})

		c.Convey("has correct replication info after load", FailureHalts, func(c C) {
			_, err = db.Add(ctx, []byte("hello"))
			c.So(err, ShouldBeNil)

			c.So(db.Close(), ShouldBeNil)

			db, err = orbitdb1.Log(ctx, "replication status tests", nil)
			c.So(err, ShouldBeNil)

			c.So(db.Load(ctx, infinity), ShouldBeNil)
			c.So(db.ReplicationStatus().GetBuffered(), ShouldEqual, 0)
			c.So(db.ReplicationStatus().GetQueued(), ShouldEqual, 0)
			c.So(db.ReplicationStatus().GetProgress(), ShouldEqual, 1)
			c.So(db.ReplicationStatus().GetMax(), ShouldEqual, 1)

			c.Convey("has correct replication info after close", FailureHalts, func(c C) {
				c.So(db.Close(), ShouldBeNil)
				c.So(db.ReplicationStatus().GetBuffered(), ShouldEqual, 0)
				c.So(db.ReplicationStatus().GetQueued(), ShouldEqual, 0)
				c.So(db.ReplicationStatus().GetProgress(), ShouldEqual, 0)
				c.So(db.ReplicationStatus().GetMax(), ShouldEqual, 0)
			})

			c.Convey("has correct replication info after sync", FailureHalts, func(c C) {
				_, err = db.Add(ctx, []byte("hello2"))
				c.So(err, ShouldBeNil)

				c.So(db.ReplicationStatus().GetBuffered(), ShouldEqual, 0)
				c.So(db.ReplicationStatus().GetQueued(), ShouldEqual, 0)
				c.So(db.ReplicationStatus().GetProgress(), ShouldEqual, 2)
				c.So(db.ReplicationStatus().GetMax(), ShouldEqual, 2)

				db2, err = orbitdb2.Log(ctx, db.Address().String(), &orbitdb.CreateDBOptions{Create: &create})
				c.So(err, ShouldBeNil)

				err = db2.Sync(ctx, db.OpLog().Heads().Slice())
				c.So(err, ShouldBeNil)

				<-time.After(100 * time.Millisecond)

				c.So(db2.ReplicationStatus().GetBuffered(), ShouldEqual, 0)
				c.So(db2.ReplicationStatus().GetQueued(), ShouldEqual, 0)
				c.So(db2.ReplicationStatus().GetProgress(), ShouldEqual, 2)
				c.So(db2.ReplicationStatus().GetMax(), ShouldEqual, 2)
			})

			c.Convey("has correct replication info after loading from snapshot", FailureHalts, func(c C) {
				_, err = db.Add(ctx, []byte("hello2"))
				c.So(err, ShouldBeNil)

				_, err = basestore.SaveSnapshot(ctx, db)
				c.So(err, ShouldBeNil)

				db, err = orbitdb1.Log(ctx, "replication status tests", nil)
				c.So(err, ShouldBeNil)

				err = db.LoadFromSnapshot(ctx)
				c.So(err, ShouldBeNil)

				<-time.After(100 * time.Millisecond)

				c.So(db.ReplicationStatus().GetBuffered(), ShouldEqual, 0)
				c.So(db.ReplicationStatus().GetQueued(), ShouldEqual, 0)
				c.So(db.ReplicationStatus().GetProgress(), ShouldEqual, 2)
				c.So(db.ReplicationStatus().GetMax(), ShouldEqual, 2)
			})
		})

		if orbitdb1 != nil {
			err = orbitdb1.Close()
			c.So(err, ShouldBeNil)
		}

		if orbitdb2 != nil {
			err = orbitdb2.Close()
			c.So(err, ShouldBeNil)
		}
	})
}
