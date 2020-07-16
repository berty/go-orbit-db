package tests

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"

	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/io"
	"berty.tech/go-ipfs-log/keystore"
	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/stores/operation"
	"berty.tech/go-orbit-db/utils"
	datastore "github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/polydawn/refmt/cbor"
	"github.com/polydawn/refmt/obj/atlas"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCreateOpen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mocknet := testingMockNet(ctx)
	node, clean := testingIPFSNode(ctx, t, mocknet)
	defer clean()

	ipfs := testingCoreAPI(t, node)

	Convey("orbit-db - Create & Open", t, FailureHalts, func(c C) {
		dbPath, clean := testingTempDir(t, "db")
		defer clean()

		orbit, err := orbitdb.NewOrbitDB(ctx, ipfs, &orbitdb.NewOrbitDBOptions{Directory: &dbPath})
		c.So(err, ShouldBeNil)

		defer orbit.Close()

		c.Convey("Create", FailureHalts, func(c C) {
			c.Convey("Errors", FailureHalts, func(c C) {
				c.Convey("throws an error if given an invalid database type", FailureHalts, func(c C) {
					db, err := orbit.Create(ctx, "first", "invalid-type", nil)

					c.So(err, ShouldNotBeNil)
					c.So(err.Error(), ShouldContainSubstring, "invalid database type")
					c.So(db, ShouldBeNil)
				})

				c.Convey("throws an error if given an address instead of name", FailureHalts, func(c C) {
					db, err := orbit.Create(ctx, "/orbitdb/Qmc9PMho3LwTXSaUXJ8WjeBZyXesAwUofdkGeadFXsqMzW/first", "eventlog", nil)
					c.So(err, ShouldNotBeNil)
					c.So(err.Error(), ShouldContainSubstring, "given database name is an address")
					c.So(db, ShouldBeNil)
				})

				c.Convey("throws an error if database already exists", FailureHalts, func(c C) {
					replicate := false

					db1, err := orbit.Create(ctx, "first", "eventlog", &orbitdb.CreateDBOptions{Replicate: &replicate})
					c.So(err, ShouldBeNil)
					if db1 == nil {
						t.Fatalf("db1 should not be nil")
					}

					defer db1.Drop()
					defer db1.Close()

					db2, err := orbit.Create(ctx, "first", "eventlog", &orbitdb.CreateDBOptions{Replicate: &replicate})
					c.So(err, ShouldNotBeNil)
					c.So(db2, ShouldBeNil)
					c.So(err.Error(), ShouldContainSubstring, "already exists")
				})

				c.Convey("throws an error if database type doesn't match", FailureHalts, func(c C) {
					replicate := false

					db1, err := orbit.KeyValue(ctx, "keyvalue", &orbitdb.CreateDBOptions{Replicate: &replicate})
					c.So(err, ShouldBeNil)
					if db1 == nil {
						t.Fatalf("db1 should not be nil")
					}

					defer db1.Drop()
					defer db1.Close()

					db2, err := orbit.Log(ctx, db1.Address().String(), nil)
					c.So(err, ShouldNotBeNil)
					c.So(db2, ShouldBeNil)
					c.So(err.Error(), ShouldContainSubstring, "unable to cast store to log")
				})
			})

			c.Convey("Success", FailureHalts, func(c C) {
				replicate := false
				db1, err := orbit.Create(ctx, "second", "eventlog", &orbitdb.CreateDBOptions{Replicate: &replicate})
				c.So(err, ShouldBeNil)
				if db1 == nil {
					t.Fatalf("db1 should not be nil")
				}

				defer db1.Drop()
				defer db1.Close()

				localDataPath := path.Join(dbPath, db1.Address().GetRoot().String(), db1.Address().GetPath())

				err = db1.Close()
				c.So(err, ShouldBeNil)

				c.Convey("database has the correct address", FailureHalts, func(c C) {
					c.So(db1.Address().String(), ShouldStartWith, "/orbitdb")
					c.So(db1.Address().String(), ShouldContainSubstring, "bafy")
					c.So(db1.Address().String(), ShouldContainSubstring, "second")
				})

				c.Convey("saves the database locally", FailureHalts, func(c C) {
					_, err := os.Stat(localDataPath)
					c.So(os.IsNotExist(err), ShouldBeFalse)
				})

				c.Convey("saves database manifest reference locally", FailureHalts, func(c C) {
					manifestHash := db1.Address().GetRoot().String()
					addr := db1.Address().String()

					ds, err := leveldb.NewDatastore(localDataPath, &leveldb.Options{ReadOnly: true})
					c.So(err, ShouldBeNil)

					defer ds.Close()

					val, err := ds.Get(datastore.NewKey(fmt.Sprintf("%s/_manifest", addr)))
					c.So(err, ShouldBeNil)

					data := string(val)

					c.So(err, ShouldBeNil)
					c.So(data, ShouldEqual, manifestHash)
				})

				c.Convey("saves database manifest file locally", FailureHalts, func(c C) {
					manifestNode, err := io.ReadCBOR(ctx, ipfs, db1.Address().GetRoot())
					c.So(err, ShouldBeNil)

					manifest := utils.Manifest{}

					err = cbor.UnmarshalAtlased(cbor.DecodeOptions{}, manifestNode.RawData(), &manifest, atlas.MustBuild(utils.AtlasManifest))
					c.So(err, ShouldBeNil)
					c.So(manifest, ShouldNotBeNil)
					c.So(manifest.Name, ShouldEqual, "second")
					c.So(manifest.Type, ShouldEqual, "eventlog")
					c.So(manifest.AccessController, ShouldStartWith, "/ipfs")
				})

				c.Convey("can pass local database directory as an option", FailureHalts, func(c C) {
					dbPath2, clean := testingTempDir(t, "db2")
					defer clean()

					db, err := orbit.Create(ctx, "third", "eventlog", &orbitdb.CreateDBOptions{Directory: &dbPath2})
					c.So(err, ShouldBeNil)

					defer db.Drop()
					defer db.Close()

					localDataPath = path.Join(dbPath2, db.Address().GetRoot().String(), db.Address().GetPath())

					_, err = os.Stat(localDataPath)
					c.So(os.IsNotExist(err), ShouldBeFalse)
				})

				c.Convey("Access Controller", FailureHalts, func(c C) {
					c.Convey("creates an access controller and adds ourselves as writer by default", FailureHalts, func(c C) {
						db, err := orbit.Create(ctx, "fourth", "eventlog", nil)
						c.So(err, ShouldBeNil)

						defer db.Drop()
						defer db.Close()

						accessController := db.AccessController()
						allowed, err := accessController.GetAuthorizedByRole("write")
						c.So(err, ShouldBeNil)

						c.So(allowed, ShouldResemble, []string{orbit.Identity().ID})
					})

					c.Convey("creates an access controller and adds writers", FailureHalts, func(c C) {
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
						c.So(err, ShouldBeNil)

						defer db.Drop()
						defer db.Close()

						accessController := db.AccessController()
						allowed, err := accessController.GetAuthorizedByRole("write")
						c.So(err, ShouldBeNil)

						c.So(allowed, ShouldResemble, []string{"another-key", "yet-another-key", orbit.Identity().ID})
					})

					c.Convey("creates an access controller and doesn't add read access keys", FailureHalts, func(c C) {
						// TODO: NOOP seems bogus in JS test
					})
				})
			})
		})

		c.Convey("determineAddress", FailureHalts, func(c C) {
			c.Convey("Errors", FailureHalts, func(c C) {
				c.Convey("throws an error if given an invalid database type", FailureHalts, func(c C) {
					addr, err := orbit.DetermineAddress(ctx, "first", "invalid-type", nil)

					c.So(err, ShouldNotBeNil)
					c.So(addr, ShouldBeNil)
					c.So(err.Error(), ShouldContainSubstring, "invalid database type")
				})

				c.Convey("throws an error if given an address instead of name", FailureHalts, func(c C) {
					addr, err := orbit.DetermineAddress(ctx, "/orbitdb/Qmc9PMho3LwTXSaUXJ8WjeBZyXesAwUofdkGeadFXsqMzW/first", "eventlog", nil)

					c.So(err, ShouldNotBeNil)
					c.So(addr, ShouldBeNil)
					c.So(err.Error(), ShouldContainSubstring, "given database name is an address, give only the name of the database")
				})
			})

			c.Convey("Success", FailureHalts, func(c C) {
				replicate := false
				addr, err := orbit.DetermineAddress(ctx, "third", "eventlog", &orbitdb.DetermineAddressOptions{Replicate: &replicate})
				c.So(err, ShouldBeNil)
				c.So(addr, ShouldNotBeNil)

				localDataPath := path.Join(dbPath, addr.GetRoot().String(), addr.GetPath())

				c.Convey("does not save the address locally", FailureHalts, func(c C) {
					_, err := os.Stat(localDataPath)
					c.So(os.IsNotExist(err), ShouldBeTrue)
				})

				c.Convey("returns the address that would have been created", FailureHalts, func(c C) {
					_, err := os.Stat(localDataPath)
					c.So(os.IsNotExist(err), ShouldBeTrue)

					db, err := orbit.Create(ctx, "third", "eventlog", &orbitdb.CreateDBOptions{Replicate: &replicate})

					c.So(err, ShouldBeNil)

					defer db.Close()

					c.So(addr.String(), ShouldStartWith, "/orbitdb")
					c.So(addr.String(), ShouldContainSubstring, "bafy")
					c.So(addr.String(), ShouldEqual, db.Address().String())
				})
			})
		})

		c.Convey("Open", FailureHalts, func(c C) {
			create := true
			overwrite := true
			storeType := "eventlog"

			db, err := orbit.Open(ctx, "abc", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType})

			c.So(err, ShouldBeNil)
			if db == nil {
				t.Fatalf("db should not be nil")
			}

			defer db.Drop()
			defer db.Close()

			c.Convey("throws an error if trying to open a database with name only and 'create' is not set to 'true'", FailureHalts, func(c C) {
				create := false

				db, err := orbit.Open(ctx, "XXX", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType})
				c.So(err, ShouldNotBeNil)
				c.So(db, ShouldBeNil)
				c.So(err.Error(), ShouldContainSubstring, "'options.Create' set to 'false'. If you want to create a database, set 'options.Create' to 'true'")
			})

			c.Convey("throws an error if trying to open a database with name only and 'create' is not set to true", FailureHalts, func(c C) {
				db, err := orbit.Open(ctx, "YYY", &orbitdb.CreateDBOptions{Create: &create})

				c.So(err, ShouldNotBeNil)
				c.So(db, ShouldBeNil)
				c.So(err.Error(), ShouldContainSubstring, "database type not provided! Provide a type with 'options.StoreType'")
			})

			c.Convey("opens a database - name only", FailureHalts, func(c C) {
				db, err := orbit.Open(ctx, "abc", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType, Overwrite: &overwrite})

				c.So(err, ShouldBeNil)

				defer db.Drop()
				defer db.Close()

				c.So(db.Address().String(), ShouldStartWith, "/orbitdb")
				c.So(db.Address().String(), ShouldContainSubstring, "bafy")
				c.So(db.Address().String(), ShouldContainSubstring, "abc")
			})

			c.Convey("opens a database - with a different identity", FailureHalts, func(c C) {
				idDS, err := leveldb.NewDatastore("", nil)
				c.So(err, ShouldBeNil)

				defer idDS.Close()

				idKeystore, err := keystore.NewKeystore(idDS)
				c.So(err, ShouldBeNil)

				identity, err := identityprovider.CreateIdentity(&identityprovider.CreateIdentityOptions{ID: "test-id", Keystore: idKeystore, Type: "orbitdb"})
				c.So(err, ShouldBeNil)
				c.So(identity, ShouldNotBeNil)

				db, err = orbit.Open(ctx, "abc", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType, Overwrite: &overwrite, Identity: identity})
				c.So(err, ShouldBeNil)

				defer db.Drop()
				defer db.Close()

				c.So(db.Address().String(), ShouldStartWith, "/orbitdb")
				c.So(db.Address().String(), ShouldContainSubstring, "bafy")
				c.So(db.Address().String(), ShouldContainSubstring, "abc")
				c.So(db.Identity(), ShouldEqual, identity)
			})

			c.Convey("opens the same database - from an address", FailureHalts, func(c C) {
				db, err := orbit.Open(ctx, db.Address().String(), nil)

				c.So(err, ShouldBeNil)

				defer db.Drop()
				defer db.Close()

				c.So(db.Address().String(), ShouldStartWith, "/orbitdb")
				c.So(db.Address().String(), ShouldContainSubstring, "bafy")
				c.So(db.Address().String(), ShouldContainSubstring, "abc")
			})

			c.Convey("opens a database and adds the creator as the only writer", FailureHalts, func(c C) {
				db, err := orbit.Open(ctx, "abc", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType, Overwrite: &overwrite})

				c.So(err, ShouldBeNil)

				defer db.Drop()
				defer db.Close()

				allowed, err := db.AccessController().GetAuthorizedByRole("write")
				c.So(err, ShouldBeNil)
				c.So(len(allowed), ShouldEqual, 1)
				c.So(allowed[0], ShouldEqual, db.Identity().ID)
			})

			c.Convey("doesn't open a database if we don't have it locally", FailureHalts, func(c C) {

			})

			c.Convey("throws an error if trying to open a database locally and we don't have it", FailureHalts, func(c C) {

			})

			c.Convey("open the database and it has the added entries", FailureHalts, func(c C) {
				db, err := orbit.Open(ctx, "ZZZ", &orbitdb.CreateDBOptions{Create: &create, StoreType: &storeType})
				c.So(err, ShouldBeNil)

				defer db.Drop()
				defer db.Close()

				logStore, ok := db.(orbitdb.EventLogStore)
				c.So(ok, ShouldBeTrue)

				_, err = logStore.Add(ctx, []byte("hello1"))
				c.So(err, ShouldBeNil)

				_, err = logStore.Add(ctx, []byte("hello2"))
				c.So(err, ShouldBeNil)

				db, err = orbit.Open(ctx, db.Address().String(), nil)
				c.So(err, ShouldBeNil)

				defer db.Drop()
				defer db.Close()

				err = db.Load(ctx, -1)
				c.So(err, ShouldBeNil)

				res := make(chan operation.Operation, 100)
				infinity := -1

				err = logStore.Stream(ctx, res, &orbitdb.StreamOptions{Amount: &infinity})

				c.So(err, ShouldBeNil)
				c.So(len(res), ShouldEqual, 2)

				res1 := <-res
				res2 := <-res

				c.So(string(res1.GetValue()), ShouldEqual, "hello1")
				c.So(string(res2.GetValue()), ShouldEqual, "hello2")
			})
		})
	})
}
