package tests

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"berty.tech/go-orbit-db/accesscontroller"

	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/io"
	"berty.tech/go-ipfs-log/keystore"
	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/stores/operation"
	"berty.tech/go-orbit-db/utils"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/polydawn/refmt/cbor"
	"github.com/polydawn/refmt/obj/atlas"

	"github.com/stretchr/testify/assert"
)

func TestCreateOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	mocknet := testingMockNet(ctx)
	node, clean := testingIPFSNode(ctx, t, mocknet)
	defer clean()

	ipfs := testingCoreAPI(t, node)

	t.Run("orbit-db - Create & Open", func(t *testing.T) {
		dbPath, clean := testingTempDir(t, "db")
		defer clean()

		orbit, err := orbitdb.NewOrbitDB(ctx, ipfs, &orbitdb.NewOrbitDBOptions{Directory: &dbPath})
		assert.NoError(t, err)

		defer orbit.Close()

		t.Run("Create", func(t *testing.T) {
			t.Run("Errors", func(t *testing.T) {
				t.Run("throws an error if given an invalid database type", func(t *testing.T) {
					db, err := orbit.Create(ctx, "first", "invalid-type", nil)

					assert.NotNil(t, err)
					assert.Contains(t, err.Error(), "invalid database type")
					assert.Nil(t, db)
				})

				t.Run("throws an error if given an address instead of name", func(t *testing.T) {
					db, err := orbit.Create(ctx, "/orbitdb/Qmc9PMho3LwTXSaUXJ8WjeBZyXesAwUofdkGeadFXsqMzW/first", "eventlog", nil)
					assert.NotNil(t, err)
					assert.Contains(t, err.Error(), "given database name is an address")
					assert.Nil(t, db)
				})

				t.Run("throws an error if database already exists", func(t *testing.T) {
					replicate := false

					db1, err := orbit.Create(ctx, "first", "eventlog", &orbitdb.CreateDBOptions{Replicate: &replicate})
					assert.NoError(t, err)
					if db1 == nil {
						t.Fatalf("db1 should not be nil")
					}

					db2, err := orbit.Create(ctx, "first", "eventlog", &orbitdb.CreateDBOptions{Replicate: &replicate})
					assert.NotNil(t, err)
					assert.Nil(t, db2)
					assert.Contains(t, err.Error(), "already exists")
				})

				t.Run("throws an error if database type doesn't match", func(t *testing.T) {
					replicate := false

					db1, err := orbit.KeyValue(ctx, "keyvalue", &orbitdb.CreateDBOptions{Replicate: &replicate})
					assert.NoError(t, err)
					if db1 == nil {
						t.Fatalf("db1 should not be nil")
					}

					db2, err := orbit.Log(ctx, db1.Address().String(), nil)
					assert.NotNil(t, err)
					assert.Nil(t, db2)
					assert.Contains(t, err.Error(), "unable to cast store to log")
				})
			})

			t.Run("Success", func(t *testing.T) {
				replicate := false
				db1, err := orbit.Create(ctx, "second", "eventlog", &orbitdb.CreateDBOptions{Replicate: &replicate})
				assert.NoError(t, err)
				if db1 == nil {
					t.Fatalf("db1 should not be nil")
				}

				localDataPath := path.Join(dbPath, db1.Address().GetRoot().String(), db1.Address().GetPath())

				err = db1.Close()
				assert.NoError(t, err)

				t.Run("database has the correct address", func(t *testing.T) {
					assert.Regexp(t, "^/orbitdb", db1.Address().String())
					assert.Contains(t, db1.Address().String(), "bafy")
					assert.Contains(t, db1.Address().String(), "second")
				})

				t.Run("saves the database locally", func(t *testing.T) {
					_, err := os.Stat(localDataPath)
					assert.False(t, os.IsNotExist(err))
				})

				t.Run("saves database manifest reference locally", func(t *testing.T) {
					manifestHash := db1.Address().GetRoot().String()
					addr := db1.Address().String()

					ds, err := leveldb.NewDatastore(localDataPath, &leveldb.Options{ReadOnly: true})
					assert.NoError(t, err)

					val, err := ds.Get(datastore.NewKey(fmt.Sprintf("%s/_manifest", addr)))
					assert.NoError(t, err)

					data := string(val)

					assert.NoError(t, err)
					assert.Equal(t, manifestHash, data)
				})

				t.Run("saves database manifest file locally", func(t *testing.T) {
					manifestNode, err := io.ReadCBOR(ctx, ipfs, db1.Address().GetRoot())
					assert.NoError(t, err)

					manifest := utils.Manifest{}

					err = cbor.UnmarshalAtlased(cbor.DecodeOptions{}, manifestNode.RawData(), &manifest, atlas.MustBuild(utils.AtlasManifest))
					assert.NoError(t, err)
					assert.NotNil(t, manifest)
					assert.Equal(t, "second", manifest.Name)
					assert.Equal(t, "eventlog", manifest.Type)
					assert.Regexp(t, "^/ipfs", manifest.AccessController)
				})

				t.Run("can pass local database directory as an option", func(t *testing.T) {
					dbPath2, clean := testingTempDir(t, "db2")
					defer clean()

					db, err := orbit.Create(ctx, "third", "eventlog", &orbitdb.CreateDBOptions{Directory: &dbPath2})
					assert.NoError(t, err)

					localDataPath = path.Join(dbPath2, db.Address().GetRoot().String(), db.Address().GetPath())

					_, err = os.Stat(localDataPath)
					assert.False(t, os.IsNotExist(err))
				})

				t.Run("Access Controller", func(t *testing.T) {
					t.Run("creates an access controller and adds ourselves as writer by default", func(t *testing.T) {
						db, err := orbit.Create(ctx, "fourth", "eventlog", nil)
						assert.NoError(t, err)

						accessController := db.AccessController()
						allowed, err := accessController.GetAuthorizedByRole("write")
						assert.NoError(t, err)

						assert.Equal(t, []string{orbit.Identity().ID}, allowed)
					})

					t.Run("creates an access controller and adds writers", func(t *testing.T) {
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
						assert.NoError(t, err)

						accessController := db.AccessController()
						allowed, err := accessController.GetAuthorizedByRole("write")
						assert.NoError(t, err)

						assert.Equal(t, []string{"another-key", "yet-another-key", orbit.Identity().ID}, allowed)
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
					addr, err := orbit.DetermineAddress(ctx, "first", "invalid-type", nil)

					assert.NotNil(t, err)
					assert.Nil(t, addr)
					assert.Contains(t, err.Error(), "invalid database type")
				})

				t.Run("throws an error if given an address instead of name", func(t *testing.T) {
					addr, err := orbit.DetermineAddress(ctx, "/orbitdb/Qmc9PMho3LwTXSaUXJ8WjeBZyXesAwUofdkGeadFXsqMzW/first", "eventlog", nil)

					assert.NotNil(t, err)
					assert.Nil(t, addr)
					assert.Contains(t, err.Error(), "given database name is an address, give only the name of the database")
				})
			})

			t.Run("Success", func(t *testing.T) {
				replicate := false
				addr, err := orbit.DetermineAddress(ctx, "third", "eventlog", &orbitdb.DetermineAddressOptions{Replicate: &replicate})
				assert.NoError(t, err)
				assert.NotNil(t, addr)

				localDataPath := path.Join(dbPath, addr.GetRoot().String(), addr.GetPath())

				t.Run("does not save the address locally", func(t *testing.T) {
					_, err := os.Stat(localDataPath)
					assert.True(t, os.IsNotExist(err))
				})

				t.Run("returns the address that would have been created", func(t *testing.T) {
					_, err := os.Stat(localDataPath)
					assert.True(t, os.IsNotExist(err))

					_, err = orbit.Create(ctx, "third", "eventlog", &orbitdb.CreateDBOptions{Replicate: &replicate})

					assert.NoError(t, err)
					assert.Regexp(t, "^/orbitdb", addr.String())
					assert.Contains(t, addr.String(), "bafy")
					// assert.Equal(t, db.Address().String(), addr.String()) // @FIXME
				})
			})
		})

		t.Run("Open", func(t *testing.T) {
			create := true
			overwrite := true
			storeType := "eventlog"

			db, err := orbit.Open(ctx, "abc", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType})

			assert.NoError(t, err)
			if db == nil {
				t.Fatalf("db should not be nil")
			}

			t.Run("throws an error if trying to open a database with name only and 'create' is not set to 'true'", func(t *testing.T) {
				create := false

				db, err := orbit.Open(ctx, "XXX", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType})
				assert.NotNil(t, err)
				assert.Nil(t, db)
				assert.Contains(t, err.Error(), "'options.Create' set to 'false'. If you want to create a database, set 'options.Create' to 'true'")
			})

			t.Run("throws an error if trying to open a database with name only and 'create' is not set to true", func(t *testing.T) {
				db, err := orbit.Open(ctx, "YYY", &orbitdb.CreateDBOptions{Create: &create})

				assert.NotNil(t, err)
				assert.Nil(t, db)
				assert.Contains(t, err.Error(), "database type not provided! Provide a type with 'options.StoreType'")
			})

			t.Run("opens a database - name only", func(t *testing.T) {
				db, err := orbit.Open(ctx, "abc", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType, Overwrite: &overwrite})

				assert.NoError(t, err)
				assert.Regexp(t, "^/orbitdb", db.Address().String())
				assert.Contains(t, db.Address().String(), "bafy")
				assert.Contains(t, db.Address().String(), "abc")
			})

			t.Run("opens a database - with a different identity", func(t *testing.T) {
				idDS, err := leveldb.NewDatastore("", nil)
				assert.NoError(t, err)

				idKeystore, err := keystore.NewKeystore(idDS)
				assert.NoError(t, err)

				identity, err := identityprovider.CreateIdentity(&identityprovider.CreateIdentityOptions{ID: "test-id", Keystore: idKeystore, Type: "orbitdb"})
				assert.NoError(t, err)
				assert.NotNil(t, identity)

				db, err = orbit.Open(ctx, "abc", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType, Overwrite: &overwrite, Identity: identity})
				assert.NoError(t, err)

				assert.Regexp(t, "^/orbitdb", db.Address().String())
				assert.Contains(t, db.Address().String(), "bafy")
				assert.Contains(t, db.Address().String(), "abc")
				assert.Equal(t, identity, db.Identity())
			})

			t.Run("opens the same database - from an address", func(t *testing.T) {
				db, err := orbit.Open(ctx, db.Address().String(), nil)

				assert.NoError(t, err)
				assert.Regexp(t, "^/orbitdb", db.Address().String())
				assert.Contains(t, db.Address().String(), "bafy")
				assert.Contains(t, db.Address().String(), "abc")
			})

			t.Run("opens a database and adds the creator as the only writer", func(t *testing.T) {
				db, err := orbit.Open(ctx, "abc", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType, Overwrite: &overwrite})

				assert.NoError(t, err)
				allowed, err := db.AccessController().GetAuthorizedByRole("write")
				assert.NoError(t, err)
				assert.Equal(t, 1, len(allowed))
				assert.Equal(t, db.Identity().ID, allowed[0])
			})

			t.Run("doesn't open a database if we don't have it locally", func(t *testing.T) {

			})

			t.Run("throws an error if trying to open a database locally and we don't have it", func(t *testing.T) {

			})

			t.Run("open the database and it has the added entries", func(t *testing.T) {
				db, err := orbit.Open(ctx, "ZZZ", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType})
				assert.NoError(t, err)

				logStore, ok := db.(orbitdb.EventLogStore)
				assert.True(t, ok)

				_, err = logStore.Add(ctx, []byte("hello1"))
				assert.NoError(t, err)

				_, err = logStore.Add(ctx, []byte("hello2"))
				assert.NoError(t, err)

				db, err = orbit.Open(ctx, db.Address().String(), nil)
				assert.NoError(t, err)

				err = db.Load(ctx, -1)
				assert.NoError(t, err)

				res := make(chan operation.Operation, 100)
				infinity := -1

				err = logStore.Stream(ctx, res, &orbitdb.StreamOptions{Amount: &infinity})

				assert.NoError(t, err)
				assert.Equal(t, 2, len(res))

				res1 := <-res
				res2 := <-res

				assert.Equal(t, "hello1", string(res1.GetValue()))
				assert.Equal(t, "hello2", string(res2.GetValue()))
			})
		})

	})
}
