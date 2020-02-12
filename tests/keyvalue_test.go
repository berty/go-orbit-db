package tests

import (
	"context"
	"testing"
	"time"

	orbitdb2 "berty.tech/go-orbit-db"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestKeyValueStore(t *testing.T) {
	tmpDir, clean := testingTempDir(t, "db-keystore")
	defer clean()

	cases := []struct{ Name, Directory string }{
		{Name: "in memory", Directory: ":memory:"},
		{Name: "persistent", Directory: tmpDir},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			testingKeyValueStore(t, c.Directory)
		})
	}
}

func testingKeyValueStore(t *testing.T, dir string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	dbname := "orbit-db-tests"
	Convey("orbit-db - Key-Value Database", t, FailureHalts, func(c C) {
		mocknet := testingMockNet(ctx)

		node, clean := testingIPFSNode(ctx, t, mocknet)
		defer clean()

		db1IPFS := testingCoreAPI(t, node)

		orbitdb1, err := orbitdb2.NewOrbitDB(ctx, db1IPFS, &orbitdb2.NewOrbitDBOptions{
			Directory: &dir,
		})
		defer orbitdb1.Close()

		assert.NoError(t, err)

		db, err := orbitdb1.KeyValue(ctx, dbname, nil)
		assert.NoError(t, err)

		c.Convey("creates and opens a database", FailureHalts, func(c C) {
			db, err := orbitdb1.KeyValue(ctx, "first kv database", nil)
			assert.NoError(t, err)

			if db == nil {
				t.Fatalf("db should not be nil")
			}

			assert.Equal(t, "keyvalue", db.Type())
			assert.Equal(t, "first kv database", db.DBName())
		})

		c.Convey("put", FailureHalts, func(c C) {
			_, err := db.Put(ctx, "key1", []byte("hello1"))
			assert.NoError(t, err)

			value, err := db.Get(ctx, "key1")
			assert.NoError(t, err)
			assert.Equal(t, "hello1", string(value))
		})

		c.Convey("get", FailureHalts, func(c C) {
			_, err := db.Put(ctx, "key1", []byte("hello2"))
			assert.NoError(t, err)

			value, err := db.Get(ctx, "key1")
			assert.NoError(t, err)
			assert.Equal(t, "hello2", string(value))
		})

		c.Convey("put updates a value", FailureHalts, func(c C) {
			_, err := db.Put(ctx, "key1", []byte("hello3"))
			assert.NoError(t, err)

			_, err = db.Put(ctx, "key1", []byte("hello4"))
			assert.NoError(t, err)

			value, err := db.Get(ctx, "key1")
			assert.NoError(t, err)
			assert.Equal(t, "hello4", string(value))
		})

		c.Convey("put/get - multiple keys", FailureHalts, func(c C) {
			_, err := db.Put(ctx, "key1", []byte("hello1"))
			assert.NoError(t, err)

			_, err = db.Put(ctx, "key2", []byte("hello2"))
			assert.NoError(t, err)

			_, err = db.Put(ctx, "key3", []byte("hello3"))
			assert.NoError(t, err)

			v1, err := db.Get(ctx, "key1")
			assert.NoError(t, err)

			v2, err := db.Get(ctx, "key2")
			assert.NoError(t, err)

			v3, err := db.Get(ctx, "key3")
			assert.NoError(t, err)

			assert.Equal(t, "hello1", string(v1))
			assert.Equal(t, "hello2", string(v2))
			assert.Equal(t, "hello3", string(v3))
		})

		c.Convey("deletes a key", FailureHalts, func(c C) {
			_, err := db.Put(ctx, "key1", []byte("hello!"))
			assert.NoError(t, err)

			_, err = db.Delete(ctx, "key1")
			assert.NoError(t, err)

			value, err := db.Get(ctx, "key1")
			assert.NoError(t, err)
			assert.Equal(t, []byte(nil), value)
		})

		c.Convey("deletes a key after multiple updates", FailureHalts, func(c C) {
			_, err := db.Put(ctx, "key1", []byte("hello1"))
			assert.NoError(t, err)

			_, err = db.Put(ctx, "key1", []byte("hello2"))
			assert.NoError(t, err)

			_, err = db.Put(ctx, "key1", []byte("hello3"))
			assert.NoError(t, err)

			_, err = db.Delete(ctx, "key1")
			assert.NoError(t, err)

			value, err := db.Get(ctx, "key1")
			assert.NoError(t, err)
			assert.Equal(t, []byte(nil), value)
		})

	})
}
