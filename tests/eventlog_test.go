package tests

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores/operation"
	cid "github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func cidPtr(c cid.Cid) *cid.Cid {
	return &c
}

func TestLogDatabase(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db1Path, clean := testingTempDir(t, "db1")
	defer clean()

	// setup
	var (
		infinity int
		orbitdb1 iface.OrbitDB
	)
	setup := func(t *testing.T) func() {
		t.Helper()
		mocknet := testingMockNet(ctx)
		node, nodeClean := testingIPFSNode(ctx, t, mocknet)

		db1IPFS := testingCoreAPI(t, node)

		infinity = -1

		var err error
		orbitdb1, err = orbitdb.NewOrbitDB(ctx, db1IPFS, &orbitdb.NewOrbitDBOptions{
			Directory: &db1Path,
		})
		require.NoError(t, err)

		cleanup := func() {
			orbitdb1.Close()
			nodeClean()
		}
		return cleanup
	}

	t.Run("basic tests", func(t *testing.T) {
		defer setup(t)()
		db, err := orbitdb1.Log(ctx, "log database", nil)
		require.NoError(t, err)
		require.NotNil(t, db)

		defer db.Close()

		////// creates and opens a database
		require.Equal(t, db.Type(), "eventlog")
		require.Equal(t, db.DBName(), "log database")

		////// returns 0 items when it's a fresh database
		res := make(chan operation.Operation, 100)
		err = db.Stream(ctx, res, &orbitdb.StreamOptions{Amount: &infinity})
		require.NoError(t, err)
		require.Equal(t, len(res), 0)

		////// returns the added entry's hash, 1 entry
		op, err := db.Add(ctx, []byte("hello1"))
		require.NoError(t, err)

		ops, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})

		require.NoError(t, err)
		require.Equal(t, len(ops), 1)
		item := ops[0]

		require.Equal(t, item.GetEntry().GetHash().String(), op.GetEntry().GetHash().String())

		////// returns the added entry's hash, 2 entries
		err = db.Load(ctx, -1)
		require.NoError(t, err)

		ops, err = db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
		require.NoError(t, err)
		require.Equal(t, len(ops), 1)

		prevHash := ops[0].GetEntry().GetHash()

		op, err = db.Add(ctx, []byte("hello2"))
		require.NoError(t, err)

		ops, err = db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
		require.NoError(t, err)
		require.Equal(t, len(ops), 2)

		require.NotEqual(t, ops[1].GetEntry().GetHash().String(), prevHash.String())
		require.Equal(t, ops[1].GetEntry().GetHash().String(), op.GetEntry().GetHash().String())
	})

	t.Run("adds five items", func(t *testing.T) {
		defer setup(t)()

		db, err := orbitdb1.Log(ctx, "second database", nil)
		require.NoError(t, err)

		defer db.Close()

		for i := 1; i <= 5; i++ {
			_, err := db.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
			require.NoError(t, err)
		}

		items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
		require.NoError(t, err)
		require.Equal(t, len(items), 5)

		for i := 1; i <= 5; i++ {
			require.Equal(t, string(items[i-1].GetValue()), fmt.Sprintf("hello%d", i))
		}
	})

	t.Run("adds an item that is > 256 bytes", func(t *testing.T) {
		defer setup(t)()
		db, err := orbitdb1.Log(ctx, "third database", nil)
		require.NoError(t, err)

		defer db.Close()

		msg := bytes.Repeat([]byte("a"), 1024)

		op, err := db.Add(ctx, msg)
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(op.GetEntry().GetHash().String(), "bafy"))
	})

	t.Run("iterator & collect & options", func(t *testing.T) {
		// setup
		var (
			db        orbitdb.EventLogStore
			itemCount int
			ops       []operation.Operation
		)
		subSetup := func(t *testing.T) func() {
			t.Helper()

			setupCleanup := setup(t)
			itemCount = 5

			ops = []operation.Operation{}

			var err error
			db, err = orbitdb1.Log(ctx, "iterator tests", nil)
			require.NoError(t, err)

			for i := 0; i < itemCount; i++ {
				op, err := db.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
				require.NoError(t, err)
				ops = append(ops, op)
			}
			cleanup := func() {
				db.Close()
				setupCleanup()
			}
			return cleanup
		}

		t.Run("iterator", func(t *testing.T) {
			t.Run("defaults", func(t *testing.T) {
				t.Run("returns an item with the correct structure", func(t *testing.T) {
					defer subSetup(t)()
					ch := make(chan operation.Operation, 100)

					err := db.Stream(ctx, ch, nil)
					require.NoError(t, err)

					next := <-ch

					require.NotNil(t, next)
					require.True(t, strings.HasPrefix(next.GetEntry().GetHash().String(), "bafy"))
					require.Nil(t, next.GetKey())
					require.Equal(t, string(next.GetValue()), "hello4")
				})

				t.Run("implements Iterator interface", func(t *testing.T) {
					defer subSetup(t)()
					ch := make(chan operation.Operation, 100)

					err := db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &infinity})
					require.NoError(t, err)
					require.Equal(t, len(ch), itemCount)
				})

				t.Run("returns 1 item as default", func(t *testing.T) {
					defer subSetup(t)()
					ch := make(chan operation.Operation, 100)

					err := db.Stream(ctx, ch, nil)
					require.NoError(t, err)

					first := <-ch
					second := <-ch

					require.Equal(t, first.GetEntry().GetHash().String(), ops[len(ops)-1].GetEntry().GetHash().String())
					require.Equal(t, second, nil)
					require.Equal(t, string(first.GetValue()), "hello4")
				})

				t.Run("returns items in the correct order", func(t *testing.T) {
					defer subSetup(t)()
					ch := make(chan operation.Operation, 100)

					amount := 3

					err := db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &amount})
					require.NoError(t, err)

					i := len(ops) - amount

					for op := range ch {
						require.Equal(t, string(op.GetValue()), fmt.Sprintf("hello%d", i))
						i++
					}
				})
			})
		})

		t.Run("collect", func(t *testing.T) {
			t.Run("returns all items", func(t *testing.T) {
				defer subSetup(t)()

				messages, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})

				require.NoError(t, err)
				require.Equal(t, len(messages), len(ops))
				require.Equal(t, string(messages[0].GetValue()), "hello0")
				require.Equal(t, string(messages[len(messages)-1].GetValue()), "hello4")
			})

			t.Run("returns 1 item", func(t *testing.T) {
				defer subSetup(t)()
				messages, err := db.List(ctx, nil)

				require.NoError(t, err)
				require.Equal(t, len(messages), 1)
			})

			t.Run("returns 3 items", func(t *testing.T) {
				defer subSetup(t)()
				three := 3
				messages, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &three})

				require.NoError(t, err)
				require.Equal(t, len(messages), 3)
			})
		})

		t.Run("Options: limit", func(t *testing.T) {
			t.Run("returns 1 item when limit is 0", func(t *testing.T) {
				defer subSetup(t)()
				ch := make(chan operation.Operation, 100)
				zero := 0
				err := db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &zero})
				require.NoError(t, err)

				require.Equal(t, len(ch), 1)

				first := <-ch
				second := <-ch

				require.Equal(t, first.GetEntry().GetHash().String(), ops[len(ops)-1].GetEntry().GetHash().String())
				require.Nil(t, second)
			})

			t.Run("returns 1 item when limit is 1", func(t *testing.T) {
				defer subSetup(t)()
				ch := make(chan operation.Operation, 100)
				one := 1
				err := db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &one})
				require.NoError(t, err)

				require.Equal(t, len(ch), 1)

				first := <-ch
				second := <-ch

				require.Equal(t, first.GetEntry().GetHash().String(), ops[len(ops)-1].GetEntry().GetHash().String())
				require.Nil(t, second)
			})

			t.Run("returns 3 items", func(t *testing.T) {
				defer subSetup(t)()
				ch := make(chan operation.Operation, 100)
				three := 3
				err := db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &three})
				require.NoError(t, err)

				require.Equal(t, len(ch), 3)

				first := <-ch
				second := <-ch
				third := <-ch
				fourth := <-ch

				require.Equal(t, first.GetEntry().GetHash().String(), ops[len(ops)-3].GetEntry().GetHash().String())
				require.Equal(t, second.GetEntry().GetHash().String(), ops[len(ops)-2].GetEntry().GetHash().String())
				require.Equal(t, third.GetEntry().GetHash().String(), ops[len(ops)-1].GetEntry().GetHash().String())
				require.Nil(t, fourth)
			})

			t.Run("returns all items", func(t *testing.T) {
				defer subSetup(t)()
				ch := make(chan operation.Operation, 100)
				err := db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &infinity})
				require.NoError(t, err)
				require.Equal(t, len(ops), len(ch))

				var last operation.Operation
				for e := range ch {
					last = e
				}

				require.Equal(t, last.GetEntry().GetHash().String(), ops[len(ops)-1].GetEntry().GetHash().String())
			})

			t.Run("returns all items when limit is bigger than -1", func(t *testing.T) {
				defer subSetup(t)()
				ch := make(chan operation.Operation, 100)
				minusThreeHundred := -300
				err := db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &minusThreeHundred})
				require.NoError(t, err)

				require.Equal(t, len(ops), len(ch))

				var last operation.Operation
				for e := range ch {
					last = e
				}

				require.Equal(t, last.GetEntry().GetHash().String(), ops[len(ops)-1].GetEntry().GetHash().String())
			})

			t.Run("returns all items when limit is bigger than number of items", func(t *testing.T) {
				defer subSetup(t)()
				ch := make(chan operation.Operation, 100)
				threeHundred := 300
				err := db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &threeHundred})
				require.NoError(t, err)

				require.Equal(t, len(ops), len(ch))

				var last operation.Operation
				for e := range ch {
					last = e
				}

				require.Equal(t, last.GetEntry().GetHash().String(), ops[len(ops)-1].GetEntry().GetHash().String())
			})
		})

		t.Run("Options: ranges", func(t *testing.T) {
			t.Run("gt & gte", func(t *testing.T) {
				t.Run("returns 1 item when gte is the head", func(t *testing.T) {
					defer subSetup(t)()
					messages, err := db.List(ctx, &orbitdb.StreamOptions{GTE: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &infinity})
					require.NoError(t, err)

					require.Equal(t, len(messages), 1)
					require.Equal(t, messages[0].GetEntry().GetHash().String(), ops[len(ops)-1].GetEntry().GetHash().String())
				})
				t.Run("returns 0 items when gt is the head", func(t *testing.T) {
					defer subSetup(t)()
					messages, err := db.List(ctx, &orbitdb.StreamOptions{GT: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &infinity})
					require.NoError(t, err)

					require.Equal(t, len(messages), 0)
				})
				t.Run("returns 2 item when gte is defined", func(t *testing.T) {
					defer subSetup(t)()
					gte := ops[len(ops)-2].GetEntry().GetHash()

					messages, err := db.List(ctx, &orbitdb.StreamOptions{GTE: &gte, Amount: &infinity})
					require.NoError(t, err)

					require.Equal(t, len(messages), 2)
					require.Equal(t, messages[0].GetEntry().GetHash().String(), ops[len(ops)-2].GetEntry().GetHash().String())
					require.Equal(t, messages[1].GetEntry().GetHash().String(), ops[len(ops)-1].GetEntry().GetHash().String())
				})
				t.Run("returns all items when gte is the root item", func(t *testing.T) {
					defer subSetup(t)()
					messages, err := db.List(ctx, &orbitdb.StreamOptions{GTE: cidPtr(ops[0].GetEntry().GetHash()), Amount: &infinity})
					require.NoError(t, err)

					require.Equal(t, len(messages), len(ops))
					require.Equal(t, messages[0].GetEntry().GetHash().String(), ops[0].GetEntry().GetHash().String())
					require.Equal(t, messages[len(messages)-1].GetEntry().GetHash().String(), ops[len(ops)-1].GetEntry().GetHash().String())
				})
				t.Run("returns items when gt is the root item", func(t *testing.T) {
					defer subSetup(t)()
					messages, err := db.List(ctx, &orbitdb.StreamOptions{GT: cidPtr(ops[0].GetEntry().GetHash()), Amount: &infinity})
					require.NoError(t, err)

					require.Equal(t, len(messages), len(ops)-1)
					require.Equal(t, messages[0].GetEntry().GetHash().String(), ops[1].GetEntry().GetHash().String())
					require.Equal(t, messages[len(messages)-1].GetEntry().GetHash().String(), ops[len(ops)-1].GetEntry().GetHash().String())
				})
				t.Run("returns items when gt is defined", func(t *testing.T) {
					defer subSetup(t)()
					messages, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
					require.NoError(t, err)
					require.Equal(t, len(messages), 5)

					gt := messages[2].GetEntry().GetHash()
					hundred := 100

					messages2, err := db.List(ctx, &orbitdb.StreamOptions{GT: &gt, Amount: &hundred})
					require.NoError(t, err)

					require.Equal(t, len(messages2), 2)
					require.Equal(t, messages2[0].GetEntry().GetHash().String(), messages[len(messages)-2].GetEntry().GetHash().String())
					require.Equal(t, messages2[1].GetEntry().GetHash().String(), messages[len(messages)-1].GetEntry().GetHash().String())
				})
			})

			t.Run("lt & lte", func(t *testing.T) {
				t.Run("returns one item after head when lt is the head", func(t *testing.T) {
					defer subSetup(t)()
					messages, err := db.List(ctx, &orbitdb.StreamOptions{LT: cidPtr(ops[len(ops)-1].GetEntry().GetHash())})
					require.NoError(t, err)

					require.Equal(t, len(messages), 1)
					require.Equal(t, messages[0].GetEntry().GetHash().String(), ops[len(ops)-2].GetEntry().GetHash().String())
				})
				t.Run("returns all items when lt is head and limit is -1", func(t *testing.T) {
					defer subSetup(t)()
					messages, err := db.List(ctx, &orbitdb.StreamOptions{LT: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &infinity})
					require.NoError(t, err)

					require.Equal(t, len(messages), len(ops)-1)
					require.Equal(t, messages[0].GetEntry().GetHash().String(), ops[0].GetEntry().GetHash().String())
					require.Equal(t, messages[len(messages)-1].GetEntry().GetHash().String(), ops[len(ops)-2].GetEntry().GetHash().String())
				})
				t.Run("returns 3 items when lt is head and limit is 3", func(t *testing.T) {
					defer subSetup(t)()
					three := 3
					messages, err := db.List(ctx, &orbitdb.StreamOptions{LT: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &three})
					require.NoError(t, err)

					require.Equal(t, len(messages), 3)
					require.Equal(t, messages[0].GetEntry().GetHash().String(), ops[len(ops)-4].GetEntry().GetHash().String())
					require.Equal(t, messages[2].GetEntry().GetHash().String(), ops[len(ops)-2].GetEntry().GetHash().String())
				})
				t.Run("returns null when lt is the root item", func(t *testing.T) {
					defer subSetup(t)()
					messages, err := db.List(ctx, &orbitdb.StreamOptions{LT: cidPtr(ops[0].GetEntry().GetHash())})
					require.NoError(t, err)
					require.Equal(t, len(messages), 0)
				})
				t.Run("returns one item when lte is the root item", func(t *testing.T) {
					defer subSetup(t)()
					messages, err := db.List(ctx, &orbitdb.StreamOptions{LTE: cidPtr(ops[0].GetEntry().GetHash())})
					require.NoError(t, err)
					require.Equal(t, len(messages), 1)
					require.Equal(t, messages[0].GetEntry().GetHash().String(), ops[0].GetEntry().GetHash().String())
				})
				t.Run("returns all items when lte is the head", func(t *testing.T) {
					defer subSetup(t)()
					messages, err := db.List(ctx, &orbitdb.StreamOptions{LTE: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &infinity})
					require.NoError(t, err)
					require.Equal(t, len(messages), itemCount)
					require.Equal(t, messages[0].GetEntry().GetHash().String(), ops[0].GetEntry().GetHash().String())
					require.Equal(t, messages[4].GetEntry().GetHash().String(), ops[itemCount-1].GetEntry().GetHash().String())
				})
				t.Run("returns 3 items when lte is the head", func(t *testing.T) {
					defer subSetup(t)()
					three := 3
					messages, err := db.List(ctx, &orbitdb.StreamOptions{LTE: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &three})
					require.NoError(t, err)
					require.Equal(t, len(messages), three)
					require.Equal(t, messages[0].GetEntry().GetHash().String(), ops[itemCount-3].GetEntry().GetHash().String())
					require.Equal(t, messages[1].GetEntry().GetHash().String(), ops[itemCount-2].GetEntry().GetHash().String())
					require.Equal(t, messages[2].GetEntry().GetHash().String(), ops[itemCount-1].GetEntry().GetHash().String())
				})
			})
		})
	})
}
