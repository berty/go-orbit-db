package tests

import (
	"context"
	"github.com/berty/go-orbit-db/orbitdb"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"path"
	"testing"
	"time"
)

func TestKeyValueStore(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	dbPath := "./orbitdb/tests/keystore"
	dbname := "orbit-db-tests"

	Convey("orbit-db - Key-Value Database", t, FailureHalts, func(c C) {
		err := os.RemoveAll(dbPath)
		c.So(err, ShouldBeNil)

		db1Path := path.Join(dbPath, "1")
		_, db1IPFS := MakeIPFS(ctx, t)

		orbitdb1, err := orbitdb.NewOrbitDB(ctx, db1IPFS, &orbitdb.NewOrbitDBOptions{
			Directory: &db1Path,
		})
		defer orbitdb1.Close()

		c.So(err, ShouldBeNil)

		db, err := orbitdb1.KeyValue(ctx, dbname, nil)
		c.So(err, ShouldBeNil)

		c.Convey("creates and opens a database", FailureHalts, func(c C) {
			db, err := orbitdb1.KeyValue(ctx, "first kv database", nil)
			c.So(err, ShouldBeNil)

			c.So(db, ShouldNotBeNil)
			c.So(db.Type(), ShouldEqual, "keyvalue")
			c.So(db.DBName(), ShouldEqual, "first kv database")
		})

		c.Convey("put", FailureHalts, func (c C) {
			_, err := db.Put(ctx, "key1", []byte("hello1"))
			c.So(err, ShouldBeNil)

			value, err := db.Get(ctx, "key1")
			c.So(err, ShouldBeNil)
			c.So(string(value), ShouldEqual, "hello1")
		})

		c.Convey("get", FailureHalts, func (c C) {
			_, err := db.Put(ctx, "key1", []byte("hello2"))
			c.So(err, ShouldBeNil)

			value, err := db.Get(ctx, "key1")
			c.So(err, ShouldBeNil)
			c.So(string(value), ShouldEqual, "hello2")
		})

		c.Convey("put updates a value", FailureHalts, func (c C) {
			_, err := db.Put(ctx, "key1", []byte("hello3"))
			c.So(err, ShouldBeNil)

			_, err = db.Put(ctx, "key1", []byte("hello4"))
			c.So(err, ShouldBeNil)

			value, err := db.Get(ctx, "key1")
			c.So(err, ShouldBeNil)
			c.So(string(value), ShouldEqual, "hello4")
		})

		c.Convey("put/get - multiple keys", FailureHalts, func (c C) {
			_, err := db.Put(ctx, "key1", []byte("hello1"))
			c.So(err, ShouldBeNil)

			_, err = db.Put(ctx, "key2", []byte("hello2"))
			c.So(err, ShouldBeNil)

			_, err = db.Put(ctx, "key3", []byte("hello3"))
			c.So(err, ShouldBeNil)

			v1, err := db.Get(ctx, "key1")
			c.So(err, ShouldBeNil)

			v2, err := db.Get(ctx, "key2")
			c.So(err, ShouldBeNil)

			v3, err := db.Get(ctx, "key3")
			c.So(err, ShouldBeNil)

			c.So(string(v1), ShouldEqual, "hello1")
			c.So(string(v2), ShouldEqual, "hello2")
			c.So(string(v3), ShouldEqual, "hello3")
		})

		c.Convey("deletes a key", FailureHalts, func(c C) {
			_, err := db.Put(ctx, "key1", []byte("hello!"))
			c.So(err, ShouldBeNil)

			_, err = db.Delete(ctx, "key1")
			c.So(err, ShouldBeNil)

			value, err := db.Get(ctx, "key1")
			c.So(err, ShouldBeNil)
			c.So(value, ShouldEqual, nil)
		})

		c.Convey("deletes a key after multiple updates", FailureHalts, func(c C) {
			_, err := db.Put(ctx, "key1", []byte("hello1"))
			c.So(err, ShouldBeNil)

			_, err = db.Put(ctx, "key1", []byte("hello2"))
			c.So(err, ShouldBeNil)

			_, err = db.Put(ctx, "key1", []byte("hello3"))
			c.So(err, ShouldBeNil)

			_, err = db.Delete(ctx, "key1")
			c.So(err, ShouldBeNil)

			value, err := db.Get(ctx, "key1")
			c.So(err, ShouldBeNil)
			c.So(value, ShouldEqual, nil)
		})

		TeardownNetwork()
	})
}
