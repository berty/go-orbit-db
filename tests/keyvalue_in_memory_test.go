package tests

import (
	"context"
	"testing"
	"time"

	orbitdb2 "berty.tech/go-orbit-db"
	. "github.com/smartystreets/goconvey/convey"
)

func TestKeyValueStoreInMemory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	dbPath := ":memory:"
	dbname := "orbit-db-tests"

	Convey("orbit-db - Key-Value Database", t, FailureHalts, func(c C) {
		mocknet := testingMockNet(ctx)

		node, clean := testingIPFSNode(ctx, t, mocknet)
		defer clean()

		db1IPFS := testingCoreAPI(t, node)

		orbitdb1, err := orbitdb2.NewOrbitDB(ctx, db1IPFS, &orbitdb2.NewOrbitDBOptions{
			Directory: &dbPath,
		})
		defer orbitdb1.Close()

		c.So(err, ShouldBeNil)

		db, err := orbitdb1.KeyValue(ctx, dbname, nil)
		c.So(err, ShouldBeNil)

		c.Convey("creates and opens a database", FailureHalts, func(c C) {
			db, err := orbitdb1.KeyValue(ctx, "first kv database", nil)
			c.So(err, ShouldBeNil)

			if db == nil {
				t.Fatalf("db should not be nil")
			}

			c.So(db.Type(), ShouldEqual, "keyvalue")
			c.So(db.DBName(), ShouldEqual, "first kv database")
		})

		c.Convey("put", FailureHalts, func(c C) {
			_, err := db.Put(ctx, "key1", []byte("hello1"))
			c.So(err, ShouldBeNil)

			value, err := db.Get(ctx, "key1")
			c.So(err, ShouldBeNil)
			c.So(string(value), ShouldEqual, "hello1")
		})

		c.Convey("get", FailureHalts, func(c C) {
			_, err := db.Put(ctx, "key1", []byte("hello2"))
			c.So(err, ShouldBeNil)

			value, err := db.Get(ctx, "key1")
			c.So(err, ShouldBeNil)
			c.So(string(value), ShouldEqual, "hello2")
		})

		c.Convey("put updates a value", FailureHalts, func(c C) {
			_, err := db.Put(ctx, "key1", []byte("hello3"))
			c.So(err, ShouldBeNil)

			_, err = db.Put(ctx, "key1", []byte("hello4"))
			c.So(err, ShouldBeNil)

			value, err := db.Get(ctx, "key1")
			c.So(err, ShouldBeNil)
			c.So(string(value), ShouldEqual, "hello4")
		})

		c.Convey("put/get - multiple keys", FailureHalts, func(c C) {
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

	})
}
