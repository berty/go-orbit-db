package tests

import (
	"context"
	"fmt"

	"berty.tech/go-orbit-db/accesscontroller"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/stores"
	"berty.tech/go-orbit-db/stores/operation"

	//"berty.tech/go-orbit-db/stores/operation"
	"testing"
	"time"

	peerstore "github.com/libp2p/go-libp2p-peerstore"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"
)

func TestReplicateAutomatically(t *testing.T) {
	Convey("orbit-db - Replication", t, FailureHalts, func(c C) {
		var db1, db2 orbitdb.EventLogStore
		var db3, db4 orbitdb.KeyValueStore

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*180)
		defer cancel()

		dbPath1, clean := testingTempDir(t, "db1")
		defer clean()

		dbPath2, clean := testingTempDir(t, "db2")
		defer clean()

		mocknet := testingMockNet(ctx)

		node1, clean := testingIPFSNode(ctx, t, mocknet)
		defer clean()

		node2, clean := testingIPFSNode(ctx, t, mocknet)
		defer clean()

		ipfs1 := testingCoreAPI(t, node1)
		ipfs2 := testingCoreAPI(t, node2)

		zap.L().Named("orbitdb.tests").Debug(fmt.Sprintf("node1 is %s", node1.Identity.String()))
		zap.L().Named("orbitdb.tests").Debug(fmt.Sprintf("node2 is %s", node2.Identity.String()))

		_, err := mocknet.LinkPeers(node1.Identity, node2.Identity)
		c.So(err, ShouldBeNil)

		peerInfo2 := peerstore.PeerInfo{ID: node2.Identity, Addrs: node2.PeerHost.Addrs()}
		err = ipfs1.Swarm().Connect(ctx, peerInfo2)
		c.So(err, ShouldBeNil)

		peerInfo1 := peerstore.PeerInfo{ID: node1.Identity, Addrs: node1.PeerHost.Addrs()}
		err = ipfs2.Swarm().Connect(ctx, peerInfo1)
		c.So(err, ShouldBeNil)

		orbitdb1, err := orbitdb.NewOrbitDB(ctx, ipfs1, &orbitdb.NewOrbitDBOptions{Directory: &dbPath1})
		c.So(err, ShouldBeNil)

		orbitdb2, err := orbitdb.NewOrbitDB(ctx, ipfs2, &orbitdb.NewOrbitDBOptions{Directory: &dbPath2})
		c.So(err, ShouldBeNil)

		access := &accesscontroller.CreateAccessControllerOptions{
			Access: map[string][]string{
				"write": {
					orbitdb1.Identity().ID,
					orbitdb2.Identity().ID,
				},
			},
		}

		db1, err = orbitdb1.Log(ctx, "replicate-automatically-tests", &orbitdb.CreateDBOptions{
			Directory:        &dbPath1,
			AccessController: access,
		})
		c.So(err, ShouldBeNil)

		db3, err = orbitdb1.KeyValue(ctx, "replicate-automatically-tests-kv", &orbitdb.CreateDBOptions{
			Directory:        &dbPath1,
			AccessController: access,
		})
		c.So(err, ShouldBeNil)

		c.Convey("starts replicating the database when peers connect", FailureHalts, func(c C) {
			const entryCount = 10

			for i := 0; i < entryCount; i++ {
				_, err := db1.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
				c.So(err, ShouldBeNil)
			}

			db2, err = orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
				Directory:        &dbPath2,
				AccessController: access,
			})
			c.So(err, ShouldBeNil)

			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			hasAllResults := false
			go db2.Subscribe(ctx, func(evt events.Event) {
				switch evt.(type) {
				case *stores.EventReplicated:
					infinity := -1

					result1, err := db1.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					c.So(err, ShouldBeNil)

					result2, err := db2.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					c.So(err, ShouldBeNil)

					if len(result1) != len(result2) {
						return
					}

					hasAllResults = true
					for i := 0; i < len(result1); i++ {
						c.So(string(result1[i].GetValue()), ShouldEqual, string(result2[i].GetValue()))
					}
					cancel()
				}
			})

			<-ctx.Done()
			c.So(hasAllResults, ShouldBeTrue)
		})

		c.Convey("automatic replication exchanges the correct heads", FailureHalts, func(c C) {
			entryCount := 33

			for i := 0; i < entryCount; i++ {
				_, err := db1.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
				c.So(err, ShouldBeNil)
			}

			db2, err := orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
				Directory:        &dbPath2,
				AccessController: access,
			})
			c.So(err, ShouldBeNil)

			db4, err := orbitdb2.KeyValue(ctx, db3.Address().String(), &orbitdb.CreateDBOptions{
				Directory:        &dbPath2,
				AccessController: access,
			})
			c.So(err, ShouldBeNil)

			ctx, cancel := context.WithTimeout(ctx, time.Second*10)
			defer cancel()

			hasAllResults := false

			infinity := -1

			go db4.Subscribe(ctx, func(event events.Event) {
				switch event.(type) {
				case *stores.EventReplicated:
					c.So("", ShouldEqual, "Should not happen")
					cancel()
				}
			})

			go db2.Subscribe(ctx, func(event events.Event) {
				switch event.(type) {
				case *stores.EventReplicateProgress:
					e := event.(*stores.EventReplicateProgress)

					op, err := operation.ParseOperation(e.Entry)
					c.So(err, ShouldBeNil)

					c.So(op.GetOperation(), ShouldEqual, "ADD")
					c.So(op.GetKey(), ShouldBeNil)
					c.So(string(op.GetValue()), ShouldStartWith, "hello")
					c.So(e.Entry.GetClock(), ShouldNotBeNil)

				case *stores.EventReplicated:
					result1, err := db1.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					c.So(err, ShouldBeNil)

					result2, err := db2.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					c.So(err, ShouldBeNil)

					if len(result1) != len(result2) {
						return
					}

					hasAllResults = true
					for i := 0; i < len(result1); i++ {
						c.So(string(result1[i].GetValue()), ShouldEqual, string(result2[i].GetValue()))
					}

					<-time.After(2 * time.Second) // Grace period so db4 EventReplicated can be received
					cancel()
				}
			})

			<-ctx.Done()

			c.So(hasAllResults, ShouldBeTrue)
		})

		if db1 != nil {
			err = db1.Drop()
			c.So(err, ShouldBeNil)
		}

		if db2 != nil {
			err = db2.Drop()
			c.So(err, ShouldBeNil)
		}

		if db3 != nil {
			err = db3.Drop()
			c.So(err, ShouldBeNil)
		}

		if db4 != nil {
			err = db4.Drop()
			c.So(err, ShouldBeNil)
		}

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
