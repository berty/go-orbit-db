package tests

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	orbitdb "github.com/stateless-minds/go-orbit-db"
	"github.com/stateless-minds/go-orbit-db/iface"
	"github.com/stretchr/testify/require"
)

// go test -benchmem -run='^$' -bench '^BenchmarkKeyValueStore$' github.com/stateless-minds/go-orbit-db/tests -v -count=1 -benchtime=1000x
func BenchmarkKeyValueStore(b *testing.B) {
	tmpDir, clean := testingTempDirB(b, "db-keystore")
	defer clean()

	cases := []struct{ Name, Directory string }{
		{Name: "in memory", Directory: ":memory:"},
		{Name: "persistent", Directory: tmpDir},
	}

	for _, c := range cases {
		b.Run(c.Name, func(b *testing.B) {
			testingKeyValueStoreB(b, c.Directory)
		})
	}
}

// go test -benchmem -run='^$' -bench '^BenchmarkKeyValueStorePB$' github.com/stateless-minds/go-orbit-db/tests -v -count=1 -benchtime=1000x
func BenchmarkKeyValueStorePB(b *testing.B) {
	tmpDir, clean := testingTempDirB(b, "db-keystore")
	defer clean()

	cases := []struct{ Name, Directory string }{
		{Name: "in memory", Directory: ":memory:"},
		{Name: "persistent", Directory: tmpDir},
	}
	
	// Parallel 
	for _, c := range cases {
		b.Run(c.Name, func(b *testing.B) {
			testingKeyValueStorePB(b, c.Directory)
		})
	}
}

func setupTestingKeyValueStoreB(ctx context.Context, b *testing.B, dir string) (iface.OrbitDB, iface.KeyValueStore, func()) {
	b.Helper()

	mocknet := testingMockNetB(b)
	node, nodeClean := testingIPFSNodeB(ctx, b, mocknet)

	db1IPFS := testingCoreAPIB(b, node)

	odb, err := orbitdb.NewOrbitDB(ctx, db1IPFS, &orbitdb.NewOrbitDBOptions{
		Directory: &dir,
	})
	require.NoError(b, err)

	db, err := odb.KeyValue(ctx, "orbit-db-tests", nil)
	require.NoError(b, err)

	cleanup := func() {
		nodeClean()
		odb.Close()
		db.Close()
	}
	return odb, db, cleanup
}

func testingKeyValueStoreB(b *testing.B, dir string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Run("put", func(b *testing.B) {
		_, db, cleanup := setupTestingKeyValueStoreB(ctx, b, dir)
		defer cleanup()
		
		b.ResetTimer()
		start := time.Now()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key%d", i)
			value := []byte(fmt.Sprintf("hello%d", i))
			_, err := db.Put(ctx, key, value)
			require.NoError(b, err)
		}
		elapsed := time.Since(start)
		tps := float64(b.N) / elapsed.Seconds()
		b.ReportMetric(tps, "tps")
		b.ReportMetric(elapsed.Seconds(), "elapsed_seconds")
	})

	b.Run("get", func(b *testing.B) {
		_, db, cleanup := setupTestingKeyValueStoreB(ctx, b, dir)
		defer cleanup()
		
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key%d", i)
			value := []byte(fmt.Sprintf("hello%d", i))
			_, err := db.Put(ctx, key, value)
			require.NoError(b, err)
		}
	
		b.ResetTimer()
		start := time.Now()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key%d", i)
			value, err := db.Get(ctx, key)
			require.NoError(b, err)
			require.Equal(b, string(value), fmt.Sprintf("hello%d", i))
		}
		elapsed := time.Since(start)
		tps := float64(b.N) / elapsed.Seconds()
		b.ReportMetric(tps, "tps")
		b.ReportMetric(elapsed.Seconds(), "elapsed_seconds")
	})
}

func testingKeyValueStorePB(b *testing.B, dir string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Run("pb-put", func(b *testing.B) {
		_, db, cleanup := setupTestingKeyValueStoreB(ctx, b, dir)
		defer cleanup()

		b.ResetTimer()
		start := time.Now()
		var successCount int64
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("key%d", i)
				value := []byte(fmt.Sprintf("hello%d", i))
				_, err := db.Put(ctx, key, value)
				if err == nil {
					atomic.AddInt64(&successCount, 1)
				}
				i++
			}
		})
		elapsed := time.Since(start)
		tps := float64(successCount) / elapsed.Seconds()
		b.ReportMetric(tps, "tps")
		b.ReportMetric(elapsed.Seconds(), "elapsed_seconds")
	})

	b.Run("pb-get", func(b *testing.B) {
		_, db, cleanup := setupTestingKeyValueStoreB(ctx, b, dir)
		defer cleanup()

		// Populate the database
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key%d", i)
			value := []byte(fmt.Sprintf("hello%d", i))
			_, err := db.Put(ctx, key, value)
			require.NoError(b, err)
		}

		b.ResetTimer()
		start := time.Now()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("key%d", i)
				value, err := db.Get(ctx, key)
				require.NoError(b, err)
				require.Equal(b, string(value), fmt.Sprintf("hello%d", i))
				i++
			}
		})
		elapsed := time.Since(start)
		tps := float64(b.N) / elapsed.Seconds()
		b.ReportMetric(tps, "tps")
		b.ReportMetric(elapsed.Seconds(), "elapsed_seconds")
	})
}