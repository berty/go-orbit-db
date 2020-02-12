package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"berty.tech/go-orbit-db/accesscontroller"

	orbitdb "berty.tech/go-orbit-db"
	peerstore "github.com/libp2p/go-libp2p-peerstore"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestReplication(t *testing.T) {
	t.Run("orbit-db - Replication", func(t *testing.T) {
		var db1, db2 orbitdb.EventLogStore

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
		assert.NoError(t, err)

		peerInfo2 := peerstore.PeerInfo{ID: node2.Identity, Addrs: node2.PeerHost.Addrs()}
		err = ipfs1.Swarm().Connect(ctx, peerInfo2)
		assert.NoError(t, err)

		peerInfo1 := peerstore.PeerInfo{ID: node1.Identity, Addrs: node1.PeerHost.Addrs()}
		err = ipfs2.Swarm().Connect(ctx, peerInfo1)
		assert.NoError(t, err)

		orbitdb1, err := orbitdb.NewOrbitDB(ctx, ipfs1, &orbitdb.NewOrbitDBOptions{Directory: &dbPath1})
		assert.NoError(t, err)

		orbitdb2, err := orbitdb.NewOrbitDB(ctx, ipfs2, &orbitdb.NewOrbitDBOptions{Directory: &dbPath2})
		assert.NoError(t, err)

		access := &accesscontroller.CreateAccessControllerOptions{
			Access: map[string][]string{
				"write": {
					orbitdb1.Identity().ID,
					orbitdb2.Identity().ID,
				},
			},
		}

		assert.NoError(t, err)

		db1, err = orbitdb1.Log(ctx, "replication-tests", &orbitdb.CreateDBOptions{
			Directory:        &dbPath1,
			AccessController: access,
		})
		assert.NoError(t, err)

		t.Run("replicates database of 1 entry", func(t *testing.T) {
			db2, err = orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
				Directory:        &dbPath2,
				AccessController: access,
			})
			assert.NoError(t, err)

			_, err = db1.Add(ctx, []byte("hello"))
			assert.NoError(t, err)

			<-time.After(time.Millisecond * 500)
			items, err := db2.List(ctx, nil)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(items))
			assert.Equal(t, "hello", string(items[0].GetValue()))
		})

		t.Run("replicates database of 100 entries", func(t *testing.T) {
			db2, err = orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
				Directory:        &dbPath2,
				AccessController: access,
			})
			assert.NoError(t, err)

			const entryCount = 100
			infinity := -1

			for i := 0; i < entryCount; i++ {
				_, err = db1.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
				assert.NoError(t, err)
			}

			<-time.After(time.Millisecond * 2000)
			items, err := db2.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
			assert.NoError(t, err)
			assert.Equal(t, 100, len(items))
			assert.Equal(t, "hello0", string(items[0].GetValue()))
			assert.Equal(t, "hello99", string(items[len(items)-1].GetValue()))
		})

		if db1 != nil {
			err = db1.Drop()
			assert.NoError(t, err)
		}

		if db2 != nil {
			err = db2.Drop()
			assert.NoError(t, err)
		}

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
