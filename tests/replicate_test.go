package tests

import (
	"context"
	"fmt"
	iface "github.com/berty/go-orbit-db"
	"github.com/berty/go-orbit-db/accesscontroller/base"
	"github.com/berty/go-orbit-db/accesscontroller/simple"
	"github.com/berty/go-orbit-db/orbitdb"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"
	"os"
	"testing"
	"time"
)

func TestReplication(t *testing.T) {
	Convey("orbit-db - Replication", t, FailureHalts, func(c C) {
		var db1, db2 iface.EventLogStore

		ctx, _ := context.WithTimeout(context.Background(), time.Second*180)
		dbPath1 := "./orbitdb/tests/replication/1"
		dbPath2 := "./orbitdb/tests/replication/2"

		defer os.RemoveAll("./orbitdb/tests/replication/")

		ipfsd1, ipfs1 := MakeIPFS(ctx, t)
		ipfsd2, ipfs2 := MakeIPFS(ctx, t)

		zap.L().Named("orbitdb.tests").Debug(fmt.Sprintf("node1 is %s", ipfsd1.Identity.String()))
		zap.L().Named("orbitdb.tests").Debug(fmt.Sprintf("node2 is %s", ipfsd2.Identity.String()))

		_ , err := TestNetwork.LinkPeers(ipfsd1.Identity, ipfsd2.Identity)
		c.So(err, ShouldBeNil)

		peerInfo2 := peerstore.PeerInfo{ID: ipfsd2.Identity, Addrs: ipfsd2.PeerHost.Addrs()}
		err = ipfs1.Swarm().Connect(ctx, peerInfo2)
		c.So(err, ShouldBeNil)

		peerInfo1 := peerstore.PeerInfo{ID: ipfsd1.Identity, Addrs: ipfsd1.PeerHost.Addrs()}
		err = ipfs2.Swarm().Connect(ctx, peerInfo1)
		c.So(err, ShouldBeNil)

		orbitdb1, err := orbitdb.NewOrbitDB(ctx, ipfs1, &orbitdb.NewOrbitDBOptions{Directory: &dbPath1})
		c.So(err, ShouldBeNil)

		orbitdb2, err := orbitdb.NewOrbitDB(ctx, ipfs2, &orbitdb.NewOrbitDBOptions{Directory: &dbPath2})
		c.So(err, ShouldBeNil)

		access, err := simple.NewSimpleAccessController(ctx, nil, &base.CreateAccessControllerOptions{
			Access: map[string][]string{
				"write": {
					orbitdb1.Identity().ID,
					orbitdb2.Identity().ID,
				},
			},
		})

		c.So(err, ShouldBeNil)

		db1, err = orbitdb1.Log(ctx, "replication-tests", &iface.CreateDBOptions{
			Directory:        &dbPath1,
			AccessController: access,
		})
		c.So(err, ShouldBeNil)

		c.Convey("replicates database of 1 entry", FailureHalts, func(c C) {
			db2, err = orbitdb2.Log(ctx, db1.Address().String(), &iface.CreateDBOptions{
				Directory: &dbPath2,
				AccessController: access,
			})
			c.So(err, ShouldBeNil)

			_, err = db1.Add(ctx, []byte("hello"))
			c.So(err, ShouldBeNil)

			<-time.After(time.Millisecond * 500)
			items, err := db2.List(ctx, nil)
			c.So(err, ShouldBeNil)
			c.So(len(items), ShouldEqual, 1)
			c.So(string(items[0].GetValue()), ShouldEqual, "hello")
		})

		c.Convey("replicates database of 100 entries", FailureHalts, func (c C) {
			db2, err = orbitdb2.Log(ctx, db1.Address().String(), &iface.CreateDBOptions{
				Directory: &dbPath2,
				AccessController: access,
			})
			c.So(err, ShouldBeNil)

			const entryCount = 100
			infinity := -1

			for i := 0; i < entryCount; i ++ {
				_, err = db1.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
				c.So(err, ShouldBeNil)
			}

			<-time.After(time.Millisecond * 2000)
			items, err := db2.List(ctx, &iface.StreamOptions{Amount: &infinity})
			c.So(err, ShouldBeNil)
			c.So(len(items), ShouldEqual, 100)
			c.So(string(items[0].GetValue()), ShouldEqual, "hello0")
			c.So(string(items[len(items) - 1].GetValue()), ShouldEqual, "hello99")
		})

		if db1 != nil {
			err = db1.Drop()
			c.So(err, ShouldBeNil)
		}

		if db2 != nil {
			err = db2.Drop()
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

		TeardownNetwork()
	})
}
