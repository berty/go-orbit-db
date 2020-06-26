package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/pubsub/directchannel"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"
)

func TestDirectChannel(t *testing.T) {
	Convey("orbit-db - Replication using Direct Channel", t, FailureHalts, func(c C) {
		var db1, db2 orbitdb.EventLogStore

		ctx, cancel := context.WithCancel(context.Background())
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

		orbitdb1, err := orbitdb.NewOrbitDB(ctx, ipfs1, &orbitdb.NewOrbitDBOptions{
			Directory:            &dbPath1,
			DirectChannelFactory: directchannel.InitDirectChannelFactory(node1.PeerHost),
		})
		c.So(err, ShouldBeNil)

		defer orbitdb1.Close()

		orbitdb2, err := orbitdb.NewOrbitDB(ctx, ipfs2, &orbitdb.NewOrbitDBOptions{
			Directory:            &dbPath2,
			DirectChannelFactory: directchannel.InitDirectChannelFactory(node2.PeerHost),
		})
		c.So(err, ShouldBeNil)

		defer orbitdb2.Close()

		access := &accesscontroller.CreateAccessControllerOptions{
			Access: map[string][]string{
				"write": {
					orbitdb1.Identity().ID,
					orbitdb2.Identity().ID,
				},
			},
		}

		c.So(err, ShouldBeNil)

		db1, err = orbitdb1.Log(ctx, "replication-tests", &orbitdb.CreateDBOptions{
			Directory:        &dbPath1,
			AccessController: access,
		})
		c.So(err, ShouldBeNil)

		defer db1.Close()

		for _, amount := range []int{1, 10} {
			// TODO: find out why this tests fails for 100 entries on CircleCI while having  the `-race` flag on

			c.Convey(fmt.Sprintf("replicates database of %d entries", amount), FailureContinues, func(c C) {
				db2, err = orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
					Directory:        &dbPath2,
					AccessController: access,
				})
				c.So(err, ShouldBeNil)

				defer db2.Close()

				infinity := -1

				for i := 0; i < amount; i++ {
					_, err = db1.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
					c.So(err, ShouldBeNil)
				}

				items, err := db1.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
				c.So(err, ShouldBeNil)
				c.So(len(items), ShouldEqual, amount)

				<-time.After(time.Millisecond * 2000)
				items, err = db2.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
				c.So(err, ShouldBeNil)
				c.So(len(items), ShouldEqual, amount)
				c.So(string(items[0].GetValue()), ShouldEqual, "hello0")
				c.So(string(items[len(items)-1].GetValue()), ShouldEqual, fmt.Sprintf("hello%d", amount-1))
			})
		}
	})

}
