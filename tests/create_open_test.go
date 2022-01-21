package tests

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/io"
	"berty.tech/go-ipfs-log/keystore"
	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/address"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores/operation"
	"berty.tech/go-orbit-db/utils"
	datastore "github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/polydawn/refmt/cbor"
	"github.com/polydawn/refmt/obj/atlas"
	"github.com/stretchr/testify/require"
)

func TestCreateOpen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mocknet := testingMockNet(ctx)
	node, clean := testingIPFSNode(ctx, t, mocknet)
	defer clean()

	ipfs := testingCoreAPI(t, node)

	// setup
	var (
		orbit  iface.OrbitDB
		dbPath string
	)
	setup := func(t *testing.T) func() {
		t.Helper()

		var dbPathClean func()
		dbPath, dbPathClean = testingTempDir(t, "db")

		var err error
		orbit, err = orbitdb.NewOrbitDB(ctx, ipfs, &orbitdb.NewOrbitDBOptions{Directory: &dbPath})
		require.NoError(t, err)

		cleanup := func() {
			orbit.Close()
			dbPathClean()
		}
		return cleanup
	}

	t.Run("Create", func(t *testing.T) {
		t.Run("Errors", func(t *testing.T) {
			t.Run("throws an error if given an invalid database type", func(t *testing.T) {
				defer setup(t)()
				db, err := orbit.Create(ctx, "first", "invalid-type", nil)

				require.Error(t, err)
				require.Contains(t, err.Error(), "invalid database type")
				require.Nil(t, db)
			})

			t.Run("throws an error if given an address instead of name", func(t *testing.T) {
				defer setup(t)()
				db, err := orbit.Create(ctx, "/orbitdb/Qmc9PMho3LwTXSaUXJ8WjeBZyXesAwUofdkGeadFXsqMzW/first", "eventlog", nil)
				require.Error(t, err)
				require.Contains(t, err.Error(), "given database name is an address")
				require.Nil(t, db)
			})

			t.Run("throws an error if database already exists", func(t *testing.T) {
				defer setup(t)()
				replicate := false

				db1, err := orbit.Create(ctx, "first", "eventlog", &orbitdb.CreateDBOptions{Replicate: &replicate})
				require.NoError(t, err)
				require.NotNil(t, db1)

				defer db1.Drop()
				defer db1.Close()

				db2, err := orbit.Create(ctx, "first", "eventlog", &orbitdb.CreateDBOptions{Replicate: &replicate})
				require.Error(t, err)
				require.Nil(t, db2)
				require.Contains(t, err.Error(), "already exists")
			})

			t.Run("throws an error if database type doesn't match", func(t *testing.T) {
				defer setup(t)()
				replicate := false

				db1, err := orbit.KeyValue(ctx, "keyvalue", &orbitdb.CreateDBOptions{Replicate: &replicate})
				require.NoError(t, err)
				require.NotNil(t, db1)

				defer db1.Drop()
				defer db1.Close()

				db2, err := orbit.Log(ctx, db1.Address().String(), nil)
				require.Error(t, err)
				require.Nil(t, db2)
				require.Contains(t, err.Error(), "unable to cast store to log")
			})
		})

		t.Run("Success", func(t *testing.T) {
			// setup
			var (
				db1 orbitdb.Store
				//db1           iface.OrbitDB
				localDataPath string
			)
			subSetup := func(t *testing.T) func() {
				t.Helper()

				setupCleanup := setup(t)

				replicate := false
				var err error
				db1, err = orbit.Create(ctx, "second", "eventlog", &orbitdb.CreateDBOptions{Replicate: &replicate})
				require.NoError(t, err)
				require.NotNil(t, db1)

				localDataPath = path.Join(dbPath, db1.Address().GetRoot().String(), db1.Address().GetPath())

				err = db1.Close()
				require.NoError(t, err)

				cleanup := func() {
					db1.Close()
					db1.Drop()
					setupCleanup()
				}
				return cleanup
			}

			t.Run("database has the correct address", func(t *testing.T) {
				defer subSetup(t)()

				require.True(t, strings.HasPrefix(db1.Address().String(), "/orbitdb"))
				require.Contains(t, db1.Address().String(), "bafy")
				require.Contains(t, db1.Address().String(), "second")
			})

			t.Run("saves the database locally", func(t *testing.T) {
				defer subSetup(t)()

				_, err := os.Stat(localDataPath)
				require.False(t, os.IsNotExist(err))
			})

			t.Run("saves database manifest reference locally", func(t *testing.T) {
				defer subSetup(t)()

				manifestHash := db1.Address().GetRoot().String()
				addr := db1.Address().String()

				ds, err := leveldb.NewDatastore(localDataPath, &leveldb.Options{ReadOnly: true})
				require.NoError(t, err)

				defer ds.Close()

				val, err := ds.Get(ctx, datastore.NewKey(fmt.Sprintf("%s/_manifest", addr)))
				require.NoError(t, err)

				data := string(val)

				require.NoError(t, err)
				require.Equal(t, data, manifestHash)
			})

			t.Run("saves database manifest file locally", func(t *testing.T) {
				defer subSetup(t)()

				manifestNode, err := io.ReadCBOR(ctx, ipfs, db1.Address().GetRoot())
				require.NoError(t, err)

				manifest := utils.Manifest{}

				err = cbor.UnmarshalAtlased(cbor.DecodeOptions{}, manifestNode.RawData(), &manifest, atlas.MustBuild(utils.AtlasManifest))
				require.NoError(t, err)
				require.NotNil(t, manifest)
				require.Equal(t, manifest.Name, "second")
				require.Equal(t, manifest.Type, "eventlog")
				require.True(t, strings.HasPrefix(manifest.AccessController, "/ipfs"))
			})

			t.Run("can pass local database directory as an option", func(t *testing.T) {
				defer subSetup(t)()

				dbPath2, clean := testingTempDir(t, "db2")
				defer clean()

				db, err := orbit.Create(ctx, "third", "eventlog", &orbitdb.CreateDBOptions{Directory: &dbPath2})
				require.NoError(t, err)

				defer db.Drop()
				defer db.Close()

				localDataPath = path.Join(dbPath2, db.Address().GetRoot().String(), db.Address().GetPath())

				_, err = os.Stat(localDataPath)
				require.False(t, os.IsNotExist(err))
			})

			t.Run("Access Controller", func(t *testing.T) {
				t.Run("creates an access controller and adds ourselves as writer by default", func(t *testing.T) {
					defer subSetup(t)()

					db, err := orbit.Create(ctx, "fourth", "eventlog", nil)
					require.NoError(t, err)

					defer db.Drop()
					defer db.Close()

					accessController := db.AccessController()
					allowed, err := accessController.GetAuthorizedByRole("write")
					require.NoError(t, err)

					require.Equal(t, allowed, []string{orbit.Identity().ID})
				})

				t.Run("creates an access controller and adds writers", func(t *testing.T) {
					defer subSetup(t)()

					access := &accesscontroller.CreateAccessControllerOptions{
						Access: map[string][]string{
							"write": {"another-key", "yet-another-key", orbit.Identity().ID},
						},
					}

					overwrite := true

					db, err := orbit.Create(ctx, "fourth", "eventlog", &orbitdb.CreateDBOptions{
						AccessController: access,
						Overwrite:        &overwrite,
					})
					require.NoError(t, err)

					defer db.Drop()
					defer db.Close()

					accessController := db.AccessController()
					allowed, err := accessController.GetAuthorizedByRole("write")
					require.NoError(t, err)
					require.Equal(t, allowed, []string{"another-key", "yet-another-key", orbit.Identity().ID})
				})

				t.Run("creates an access controller and doesn't add read access keys", func(t *testing.T) {
					// TODO: NOOP seems bogus in JS test
				})
			})
		})
	})

	t.Run("determineAddress", func(t *testing.T) {
		t.Run("Errors", func(t *testing.T) {
			t.Run("throws an error if given an invalid database type", func(t *testing.T) {
				defer setup(t)()

				addr, err := orbit.DetermineAddress(ctx, "first", "invalid-type", nil)

				require.Error(t, err)
				require.Nil(t, addr)
				require.Contains(t, err.Error(), "invalid database type")
			})

			t.Run("throws an error if given an address instead of name", func(t *testing.T) {
				defer setup(t)()

				addr, err := orbit.DetermineAddress(ctx, "/orbitdb/Qmc9PMho3LwTXSaUXJ8WjeBZyXesAwUofdkGeadFXsqMzW/first", "eventlog", nil)

				require.Error(t, err)
				require.Nil(t, addr)
				require.Contains(t, err.Error(), "given database name is an address, give only the name of the database")
			})
		})

		t.Run("Success", func(t *testing.T) {
			// setup
			var (
				localDataPath string
				replicate     bool
				addr          address.Address
			)
			subSetup := func(t *testing.T) func() {
				t.Helper()

				setupCleanup := setup(t)
				replicate = false
				var err error
				addr, err = orbit.DetermineAddress(ctx, "third", "eventlog", &orbitdb.DetermineAddressOptions{Replicate: &replicate})
				require.NoError(t, err)
				require.NotNil(t, addr)
				localDataPath = path.Join(dbPath, addr.GetRoot().String(), addr.GetPath())
				cleanup := func() {
					setupCleanup()
				}
				return cleanup
			}

			t.Run("does not save the address locally", func(t *testing.T) {
				defer subSetup(t)()

				_, err := os.Stat(localDataPath)
				require.True(t, os.IsNotExist(err))
			})

			t.Run("returns the address that would have been created", func(t *testing.T) {
				defer subSetup(t)()
				_, err := os.Stat(localDataPath)
				require.True(t, os.IsNotExist(err))

				db, err := orbit.Create(ctx, "third", "eventlog", &orbitdb.CreateDBOptions{Replicate: &replicate})

				require.NoError(t, err)

				defer db.Close()

				require.True(t, strings.HasPrefix(addr.String(), "/orbitdb"))
				require.Contains(t, addr.String(), "bafy")
				require.Equal(t, addr.String(), db.Address().String())
			})
		})
	})

	t.Run("Open", func(t *testing.T) {
		// setup
		var (
			storeType         string
			create, overwrite bool
			db                iface.Store
		)
		subSetup := func(t *testing.T) func() {
			t.Helper()

			setupCleanup := setup(t)

			create = true
			overwrite = true
			storeType = "eventlog"
			var err error
			db, err = orbit.Open(ctx, "abc", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType})
			require.NoError(t, err)
			require.NotNil(t, db)

			cleanup := func() {
				db.Close()
				db.Drop()
				setupCleanup()
			}
			return cleanup
		}

		t.Run("throws an error if trying to open a database with name only and 'create' is not set to 'true'", func(t *testing.T) {
			defer subSetup(t)()
			create := false

			db, err := orbit.Open(ctx, "XXX", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType})
			require.Error(t, err)
			require.Nil(t, db)
			require.Contains(t, err.Error(), "'options.Create' set to 'false'. If you want to create a database, set 'options.Create' to 'true'")
		})

		t.Run("throws an error if trying to open a database with name only and 'create' is not set to true", func(t *testing.T) {
			defer subSetup(t)()
			db, err := orbit.Open(ctx, "YYY", &orbitdb.CreateDBOptions{Create: &create})

			require.Error(t, err)
			require.Nil(t, db)
			require.Contains(t, err.Error(), "database type not provided! Provide a type with 'options.StoreType'")
		})

		t.Run("opens a database - name only", func(t *testing.T) {
			defer subSetup(t)()
			db, err := orbit.Open(ctx, "abc", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType, Overwrite: &overwrite})

			require.NoError(t, err)

			defer db.Drop()
			defer db.Close()

			require.True(t, strings.HasPrefix(db.Address().String(), "/orbitdb"))
			require.Contains(t, db.Address().String(), "bafy")
			require.Contains(t, db.Address().String(), "abc")
		})

		t.Run("opens a database - with a different identity", func(t *testing.T) {
			defer subSetup(t)()
			idDS, err := leveldb.NewDatastore("", nil)
			require.NoError(t, err)

			defer idDS.Close()

			idKeystore, err := keystore.NewKeystore(idDS)
			require.NoError(t, err)

			identity, err := identityprovider.CreateIdentity(ctx, &identityprovider.CreateIdentityOptions{ID: "test-id", Keystore: idKeystore, Type: "orbitdb"})
			require.NoError(t, err)
			require.NotNil(t, identity)

			db, err = orbit.Open(ctx, "abc", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType, Overwrite: &overwrite, Identity: identity})
			require.NoError(t, err)

			defer db.Drop()
			defer db.Close()

			require.True(t, strings.HasPrefix(db.Address().String(), "/orbitdb"))
			require.Contains(t, db.Address().String(), "bafy")
			require.Contains(t, db.Address().String(), "abc")
			require.Equal(t, db.Identity(), identity)
		})

		t.Run("opens the same database - from an address", func(t *testing.T) {
			defer subSetup(t)()
			db, err := orbit.Open(ctx, db.Address().String(), nil)

			require.NoError(t, err)

			defer db.Drop()
			defer db.Close()

			require.True(t, strings.HasPrefix(db.Address().String(), "/orbitdb"))
			require.Contains(t, db.Address().String(), "bafy")
			require.Contains(t, db.Address().String(), "abc")
		})

		t.Run("opens a database and adds the creator as the only writer", func(t *testing.T) {
			defer subSetup(t)()
			db, err := orbit.Open(ctx, "abc", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType, Overwrite: &overwrite})

			require.NoError(t, err)

			defer db.Drop()
			defer db.Close()

			allowed, err := db.AccessController().GetAuthorizedByRole("write")
			require.NoError(t, err)
			require.Equal(t, len(allowed), 1)
			require.Equal(t, allowed[0], db.Identity().ID)
		})

		t.Run("doesn't open a database if we don't have it locally", func(t *testing.T) {

		})

		t.Run("throws an error if trying to open a database locally and we don't have it", func(t *testing.T) {

		})

		t.Run("open the database and it has the added entries", func(t *testing.T) {
			defer subSetup(t)()
			db, err := orbit.Open(ctx, "ZZZ", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType})
			require.NoError(t, err)

			defer db.Drop()
			defer db.Close()

			logStore, ok := db.(orbitdb.EventLogStore)
			require.True(t, ok)

			_, err = logStore.Add(ctx, []byte("hello1"))
			require.NoError(t, err)

			_, err = logStore.Add(ctx, []byte("hello2"))
			require.NoError(t, err)

			db, err = orbit.Open(ctx, db.Address().String(), nil)
			require.NoError(t, err)

			defer db.Drop()
			defer db.Close()

			err = db.Load(ctx, -1)
			require.NoError(t, err)

			res := make(chan operation.Operation, 100)
			infinity := -1

			err = logStore.Stream(ctx, res, &orbitdb.StreamOptions{Amount: &infinity})

			require.NoError(t, err)
			require.Equal(t, len(res), 2)

			res1 := <-res
			res2 := <-res

			require.Equal(t, string(res1.GetValue()), "hello1")
			require.Equal(t, string(res2.GetValue()), "hello2")
		})
	})
}
