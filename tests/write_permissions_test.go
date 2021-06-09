package tests

import (
	"context"
	"testing"
	"time"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores"
	"github.com/stretchr/testify/require"
)

func TestWritePermissions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mocknet := testingMockNet(ctx)
	node, clean := testingIPFSNode(ctx, t, mocknet)
	defer clean()
	ipfs := testingCoreAPI(t, node)

	// setup
	var orbitdb1, orbitdb2 iface.OrbitDB
	setup := func(t *testing.T) func() {
		t.Helper()

		dbPath1, dbPath1Clean := testingTempDir(t, "db1")
		dbPath2, dbPath2Clean := testingTempDir(t, "db2")

		var err error
		orbitdb1, err = orbitdb.NewOrbitDB(ctx, ipfs, &orbitdb.NewOrbitDBOptions{Directory: &dbPath1})
		require.NoError(t, err)

		orbitdb2, err = orbitdb.NewOrbitDB(ctx, ipfs, &orbitdb.NewOrbitDBOptions{Directory: &dbPath2})
		require.NoError(t, err)

		cleanup := func() {
			orbitdb1.Close()
			orbitdb2.Close()
			dbPath1Clean()
			dbPath2Clean()
		}
		return cleanup
	}

	t.Run("allows multiple peers to write to the databases", func(t *testing.T) {
		t.Run("eventlog allows multiple writers", func(t *testing.T) {
			defer setup(t)()

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
			require.NoError(t, err)
			defer db1.Close()

			db2, err := orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
				AccessController: ac,
			})
			require.NoError(t, err)
			defer db2.Close()

			_, err = db1.Add(ctx, []byte("hello"))
			require.NoError(t, err)

			_, err = db2.Add(ctx, []byte("hello"))
			require.NoError(t, err)

			values, err := db1.List(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, len(values), 1)

			require.Equal(t, string(values[0].GetValue()), "hello")

			values, err = db2.List(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, len(values), 1)

			require.Equal(t, string(values[0].GetValue()), "hello")
		})
	})

	t.Run("syncs databases", func(t *testing.T) {
		t.Run("eventlog syncs", func(t *testing.T) {
			defer setup(t)()

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
			require.NoError(t, err)
			defer db1.Close()

			db2, err := orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
				AccessController: ac,
			})
			require.NoError(t, err)
			defer db2.Close()

			_, err = db2.Add(ctx, []byte("hello"))
			require.NoError(t, err)

			require.Equal(t, db1.OpLog().Len(), 0)

			err = db1.Sync(ctx, db2.OpLog().Heads().Slice())
			require.NoError(t, err)

			<-time.After(time.Millisecond * 300)

			values, err := db1.List(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, len(values), 1)

			require.Equal(t, string(values[0].GetValue()), "hello")
		})
	})

	t.Run("syncs databases that anyone can write to", func(t *testing.T) {
		t.Run("eventlog syncs", func(t *testing.T) {
			defer setup(t)()

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
			require.NoError(t, err)
			defer db1.Close()

			db2, err := orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
				AccessController: ac,
			})
			require.NoError(t, err)
			defer db2.Close()

			_, err = db2.Add(ctx, []byte("hello"))
			require.NoError(t, err)

			require.Equal(t, db1.OpLog().Len(), 0)

			err = db1.Sync(ctx, db2.OpLog().Heads().Slice())
			require.NoError(t, err)

			<-time.After(time.Millisecond * 300)

			values, err := db1.List(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, len(values), 1)

			require.Equal(t, string(values[0].GetValue()), "hello")
		})
	})

	t.Run("doesn't sync if peer is not allowed to write to the database", func(t *testing.T) {
		t.Run("eventlog doesn't sync", func(t *testing.T) {
			defer setup(t)()

			ac := &accesscontroller.CreateAccessControllerOptions{
				Access: map[string][]string{
					"write": {orbitdb1.Identity().ID},
				},
			}

			db1, err := orbitdb1.Log(ctx, "write error test 1", &orbitdb.CreateDBOptions{
				AccessController: ac,
			})
			require.NoError(t, err)
			defer db1.Close()

			db2, err := orbitdb2.Log(ctx, "write error test 1", &orbitdb.CreateDBOptions{
				AccessController: ac,
			})
			require.NoError(t, err)
			defer db2.Close()

			subCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			sub := db1.Subscribe(ctx)
			go func() {
				for evt := range sub {
					switch evt.(type) {
					case *stores.EventReplicated:
						require.Equal(t, "this", "should not occur")
					}
				}

			}()

			_, err = db2.Add(ctx, []byte("hello"))
			require.Error(t, err)

			<-subCtx.Done()

			err = db1.Sync(ctx, db2.OpLog().Heads().Slice())
			require.NoError(t, err)

			<-time.After(300 * time.Millisecond)

			require.Equal(t, db1.OpLog().Len(), 0)
			require.Equal(t, db2.OpLog().Len(), 0)

			_, err = db1.Add(ctx, []byte("hello"))
			require.NoError(t, err)

			<-subCtx.Done()

			err = db2.Sync(ctx, db1.OpLog().Heads().Slice())
			require.NoError(t, err)

			<-time.After(300 * time.Millisecond)

			require.Equal(t, db1.OpLog().Len(), 1)
			require.Equal(t, db2.OpLog().Len(), 1)
		})
	})

	t.Run("throws an error if peer is not allowed to write to the database", func(t *testing.T) {
		t.Run("eventlog doesn't sync", func(t *testing.T) {
			defer setup(t)()

			ac := &accesscontroller.CreateAccessControllerOptions{
				Access: map[string][]string{
					"write": {},
				},
			}

			db1, err := orbitdb1.Log(ctx, "write error test 2", &orbitdb.CreateDBOptions{
				AccessController: ac,
			})
			require.NoError(t, err)
			defer db1.Close()

			db2, err := orbitdb2.Log(ctx, db1.Address().String(), &orbitdb.CreateDBOptions{
				AccessController: ac,
			})
			require.NoError(t, err)
			defer db2.Close()

			_, err = db2.Add(ctx, []byte("hello"))
			require.Error(t, err)
		})
	})
}
