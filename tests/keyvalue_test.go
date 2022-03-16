package tests

import (
	"context"
	"testing"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/iface"
	"github.com/stretchr/testify/require"
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

func setupTestingKeyValueStore(ctx context.Context, t *testing.T, dir string) (iface.OrbitDB, iface.KeyValueStore, func()) {
	t.Helper()

	mocknet := testingMockNet(ctx)
	node, nodeClean := testingIPFSNode(ctx, t, mocknet)

	db1IPFS := testingCoreAPI(t, node)

	odb, err := orbitdb.NewOrbitDB(ctx, db1IPFS, &orbitdb.NewOrbitDBOptions{
		Directory: &dir,
	})
	require.NoError(t, err)

	db, err := odb.KeyValue(ctx, "orbit-db-tests", nil)
	require.NoError(t, err)

	cleanup := func() {
		nodeClean()
		odb.Close()
		db.Close()
	}
	return odb, db, cleanup
}

func testingKeyValueStore(t *testing.T, dir string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("creates and opens a database", func(t *testing.T) {
		odb, _, cleanup := setupTestingKeyValueStore(ctx, t, dir)
		defer cleanup()

		db, err := odb.KeyValue(ctx, "first kv database", nil)
		require.NoError(t, err)
		require.NotNil(t, db)

		defer db.Close()

		require.Equal(t, db.Type(), "keyvalue")
		require.Equal(t, db.DBName(), "first kv database")
	})

	t.Run("put", func(t *testing.T) {
		_, db, cleanup := setupTestingKeyValueStore(ctx, t, dir)
		defer cleanup()

		_, err := db.Put(ctx, "key1", []byte("hello1"))
		require.NoError(t, err)

		value, err := db.Get(ctx, "key1")
		require.NoError(t, err)
		require.Equal(t, string(value), "hello1")
	})

	t.Run("get", func(t *testing.T) {
		_, db, cleanup := setupTestingKeyValueStore(ctx, t, dir)
		defer cleanup()

		_, err := db.Put(ctx, "key1", []byte("hello2"))
		require.NoError(t, err)

		value, err := db.Get(ctx, "key1")
		require.NoError(t, err)
		require.Equal(t, string(value), "hello2")
	})

	t.Run("put updates a value", func(t *testing.T) {
		_, db, cleanup := setupTestingKeyValueStore(ctx, t, dir)
		defer cleanup()

		_, err := db.Put(ctx, "key1", []byte("hello3"))
		require.NoError(t, err)

		_, err = db.Put(ctx, "key1", []byte("hello4"))
		require.NoError(t, err)

		value, err := db.Get(ctx, "key1")
		require.NoError(t, err)
		require.Equal(t, string(value), "hello4")
	})

	t.Run("put/get - multiple keys", func(t *testing.T) {
		_, db, cleanup := setupTestingKeyValueStore(ctx, t, dir)
		defer cleanup()

		_, err := db.Put(ctx, "key1", []byte("hello1"))
		require.NoError(t, err)

		_, err = db.Put(ctx, "key2", []byte("hello2"))
		require.NoError(t, err)

		_, err = db.Put(ctx, "key3", []byte("hello3"))
		require.NoError(t, err)

		v1, err := db.Get(ctx, "key1")
		require.NoError(t, err)

		v2, err := db.Get(ctx, "key2")
		require.NoError(t, err)

		v3, err := db.Get(ctx, "key3")
		require.NoError(t, err)

		require.Equal(t, string(v1), "hello1")
		require.Equal(t, string(v2), "hello2")
		require.Equal(t, string(v3), "hello3")
	})

	t.Run("deletes a key", func(t *testing.T) {
		_, db, cleanup := setupTestingKeyValueStore(ctx, t, dir)
		defer cleanup()

		_, err := db.Put(ctx, "key1", []byte("hello!"))
		require.NoError(t, err)

		_, err = db.Delete(ctx, "key1")
		require.NoError(t, err)

		value, err := db.Get(ctx, "key1")
		require.NoError(t, err)
		require.Nil(t, value)
	})

	t.Run("deletes a key after multiple updates", func(t *testing.T) {
		_, db, cleanup := setupTestingKeyValueStore(ctx, t, dir)
		defer cleanup()

		_, err := db.Put(ctx, "key1", []byte("hello1"))
		require.NoError(t, err)

		_, err = db.Put(ctx, "key1", []byte("hello2"))
		require.NoError(t, err)

		_, err = db.Put(ctx, "key1", []byte("hello3"))
		require.NoError(t, err)

		_, err = db.Delete(ctx, "key1")
		require.NoError(t, err)

		value, err := db.Get(ctx, "key1")
		require.NoError(t, err)
		require.Nil(t, value)
	})
}
