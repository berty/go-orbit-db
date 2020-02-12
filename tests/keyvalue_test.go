package tests

import (
	"context"
	"testing"
	"time"

	orbitdb2 "berty.tech/go-orbit-db"

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
	t.Run("orbit-db - Key-Value Database", func(t *testing.T) {
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

		t.Run("creates and opens a database", func(t *testing.T) {
			db, err := orbitdb1.KeyValue(ctx, "first kv database", nil)
			assert.NoError(t, err)

			if db == nil {
				t.Fatalf("db should not be nil")
			}

			assert.Equal(t, "keyvalue", db.Type())
			assert.Equal(t, "first kv database", db.DBName())
		})

		t.Run("put", func(t *testing.T) {
			_, err := db.Put(ctx, "key1", []byte("hello1"))
			assert.NoError(t, err)

			value, err := db.Get(ctx, "key1")
			assert.NoError(t, err)
			assert.Equal(t, "hello1", string(value))
		})

		t.Run("get", func(t *testing.T) {
			_, err := db.Put(ctx, "key1", []byte("hello2"))
			assert.NoError(t, err)

			value, err := db.Get(ctx, "key1")
			assert.NoError(t, err)
			assert.Equal(t, "hello2", string(value))
		})

		t.Run("put updates a value", func(t *testing.T) {
			_, err := db.Put(ctx, "key1", []byte("hello3"))
			assert.NoError(t, err)

			_, err = db.Put(ctx, "key1", []byte("hello4"))
			assert.NoError(t, err)

			value, err := db.Get(ctx, "key1")
			assert.NoError(t, err)
			assert.Equal(t, "hello4", string(value))
		})

		t.Run("put/get - multiple keys", func(t *testing.T) {
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

		t.Run("deletes a key", func(t *testing.T) {
			_, err := db.Put(ctx, "key1", []byte("hello!"))
			assert.NoError(t, err)

			_, err = db.Delete(ctx, "key1")
			assert.NoError(t, err)

			value, err := db.Get(ctx, "key1")
			assert.NoError(t, err)
			assert.Equal(t, []byte(nil), value)
		})

		t.Run("deletes a key after multiple updates", func(t *testing.T) {
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
