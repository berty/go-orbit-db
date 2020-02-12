package tests

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/stores/operation"
)

func cidPtr(c cid.Cid) *cid.Cid {
	return &c
}

func TestLogDatabase(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db1Path, clean := testingTempDir(t, "db1")
	defer clean()

	//.createInstance(ipfs, { directory: path.join(dbPath, '1') })
	t.Run("creates and opens a database", func(t *testing.T) {
		mocknet := testingMockNet(ctx)
		node, clean := testingIPFSNode(ctx, t, mocknet)
		defer clean()

		db1IPFS := testingCoreAPI(t, node)

		infinity := -1

		orbitdb1, err := orbitdb.NewOrbitDB(ctx, db1IPFS, &orbitdb.NewOrbitDBOptions{
			Directory: &db1Path,
		})
		assert.NoError(t, err)

		defer orbitdb1.Close()

		t.Run("basic tests", func(t *testing.T) {
			db, err := orbitdb1.Log(ctx, "log database", nil)
			assert.NoError(t, err)
			if db == nil {
				t.Fatalf("db should not be nil")
			}

			////// creates and opens a database
			assert.Equal(t, "eventlog", db.Type())
			assert.Equal(t, "log database", db.DBName())

			////// returns 0 items when it's a fresh database
			res := make(chan operation.Operation, 100)
			err = db.Stream(ctx, res, &orbitdb.StreamOptions{Amount: &infinity})
			assert.NoError(t, err)
			assert.Equal(t, 0, len(res))

			////// returns the added entry's hash, 1 entry
			op, err := db.Add(ctx, []byte("hello1"))
			assert.NoError(t, err)

			ops, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})

			assert.NoError(t, err)
			assert.Equal(t, 1, len(ops))
			item := ops[0]

			assert.Equal(t, op.GetEntry().GetHash().String(), item.GetEntry().GetHash().String())

			////// returns the added entry's hash, 2 entries
			err = db.Load(ctx, -1)
			assert.NoError(t, err)

			ops, err = db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
			assert.NoError(t, err)
			assert.Equal(t, 1, len(ops))

			prevHash := ops[0].GetEntry().GetHash()

			op, err = db.Add(ctx, []byte("hello2"))
			assert.NoError(t, err)

			ops, err = db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
			assert.NoError(t, err)
			assert.Equal(t, 2, len(ops))

			assert.NotEqual(t, prevHash.String(), ops[1].GetEntry().GetHash().String())
			assert.Equal(t, op.GetEntry().GetHash().String(), ops[1].GetEntry().GetHash().String())
		})

		t.Run("adds five items", func(t *testing.T) {
			db, err := orbitdb1.Log(ctx, "second database", nil)
			assert.NoError(t, err)

			for i := 1; i <= 5; i++ {
				_, err := db.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
				assert.NoError(t, err)
			}

			items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
			assert.NoError(t, err)
			assert.Equal(t, 5, len(items))

			for i := 1; i <= 5; i++ {
				assert.Equal(t, fmt.Sprintf("hello%d", i), string(items[i-1].GetValue()))
			}
		})

		t.Run("adds an item that is > 256 bytes", func(t *testing.T) {
			db, err := orbitdb1.Log(ctx, "third database", nil)
			assert.NoError(t, err)

			msg := bytes.Repeat([]byte("a"), 1024)

			op, err := db.Add(ctx, msg)
			assert.NoError(t, err)
			assert.Regexp(t, "^bafy", op.GetEntry().GetHash().String())
		})

		t.Run("iterator & collect & options", func(t *testing.T) {
			itemCount := 5
			var ops []operation.Operation

			db, err := orbitdb1.Log(ctx, "iterator tests", nil)
			assert.NoError(t, err)

			for i := 0; i < itemCount; i++ {
				op, err := db.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
				assert.NoError(t, err)
				ops = append(ops, op)
			}

			t.Run("iterator", func(t *testing.T) {
				t.Run("defaults", func(t *testing.T) {
					t.Run("returns an item with the correct structure", func(t *testing.T) {
						ch := make(chan operation.Operation, 100)

						err = db.Stream(ctx, ch, nil)
						assert.NoError(t, err)

						next := <-ch

						assert.NotNil(t, next)
						assert.Regexp(t, "^bafy", next.GetEntry().GetHash().String())
						assert.Nil(t, next.GetKey())
						assert.Equal(t, "hello4", string(next.GetValue()))
					})

					t.Run("implements Iterator interface", func(t *testing.T) {
						ch := make(chan operation.Operation, 100)

						err = db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &infinity})
						assert.NoError(t, err)

						assert.Equal(t, itemCount, len(ch))
					})

					t.Run("returns 1 item as default", func(t *testing.T) {
						ch := make(chan operation.Operation, 100)

						err = db.Stream(ctx, ch, nil)
						assert.NoError(t, err)

						first := <-ch
						second := <-ch

						assert.Equal(t, ops[len(ops)-1].GetEntry().GetHash().String(), first.GetEntry().GetHash().String())
						assert.Equal(t, nil, second)
						assert.Equal(t, "hello4", string(first.GetValue()))
					})

					t.Run("returns items in the correct order", func(t *testing.T) {
						ch := make(chan operation.Operation, 100)

						amount := 3

						err := db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &amount})
						assert.NoError(t, err)

						i := len(ops) - amount

						for op := range ch {
							assert.Equal(t, fmt.Sprintf("hello%d", i), string(op.GetValue()))
							i++
						}
					})
				})
			})

			t.Run("collect", func(t *testing.T) {
				t.Run("returns all items", func(t *testing.T) {
					messages, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})

					assert.NoError(t, err)
					assert.Equal(t, len(ops), len(messages))
					assert.Equal(t, "hello0", string(messages[0].GetValue()))
					assert.Equal(t, "hello4", string(messages[len(messages)-1].GetValue()))
				})

				t.Run("returns 1 item", func(t *testing.T) {
					messages, err := db.List(ctx, nil)

					assert.NoError(t, err)
					assert.Equal(t, 1, len(messages))
				})

				t.Run("returns 3 items", func(t *testing.T) {
					three := 3
					messages, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &three})

					assert.NoError(t, err)
					assert.Equal(t, 3, len(messages))
				})
			})

			t.Run("Options: limit", func(t *testing.T) {
				t.Run("returns 1 item when limit is 0", func(t *testing.T) {
					ch := make(chan operation.Operation, 100)
					zero := 0
					err = db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &zero})
					assert.NoError(t, err)

					assert.Equal(t, 1, len(ch))

					first := <-ch
					second := <-ch

					assert.Equal(t, ops[len(ops)-1].GetEntry().GetHash().String(), first.GetEntry().GetHash().String())
					assert.Nil(t, second)
				})

				t.Run("returns 1 item when limit is 1", func(t *testing.T) {
					ch := make(chan operation.Operation, 100)
					one := 1
					err = db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &one})
					assert.NoError(t, err)

					assert.Equal(t, 1, len(ch))

					first := <-ch
					second := <-ch

					assert.Equal(t, ops[len(ops)-1].GetEntry().GetHash().String(), first.GetEntry().GetHash().String())
					assert.Nil(t, second)
				})

				t.Run("returns 3 items", func(t *testing.T) {
					ch := make(chan operation.Operation, 100)
					three := 3
					err = db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &three})
					assert.NoError(t, err)

					assert.Equal(t, 3, len(ch))

					first := <-ch
					second := <-ch
					third := <-ch
					fourth := <-ch

					assert.Equal(t, ops[len(ops)-3].GetEntry().GetHash().String(), first.GetEntry().GetHash().String())
					assert.Equal(t, ops[len(ops)-2].GetEntry().GetHash().String(), second.GetEntry().GetHash().String())
					assert.Equal(t, ops[len(ops)-1].GetEntry().GetHash().String(), third.GetEntry().GetHash().String())
					assert.Nil(t, fourth)
				})

				t.Run("returns all items", func(t *testing.T) {
					ch := make(chan operation.Operation, 100)
					err = db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &infinity})
					assert.NoError(t, err)

					assert.Equal(t, len(ch), len(ops))

					var last operation.Operation
					for e := range ch {
						last = e
					}

					assert.Equal(t, ops[len(ops)-1].GetEntry().GetHash().String(), last.GetEntry().GetHash().String())
				})

				t.Run("returns all items when limit is bigger than -1", func(t *testing.T) {
					ch := make(chan operation.Operation, 100)
					minusThreeHundred := -300
					err = db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &minusThreeHundred})
					assert.NoError(t, err)

					assert.Equal(t, len(ch), len(ops))

					var last operation.Operation
					for e := range ch {
						last = e
					}

					assert.Equal(t, ops[len(ops)-1].GetEntry().GetHash().String(), last.GetEntry().GetHash().String())
				})

				t.Run("returns all items when limit is bigger than number of items", func(t *testing.T) {
					ch := make(chan operation.Operation, 100)
					threeHundred := 300
					err = db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &threeHundred})
					assert.NoError(t, err)

					assert.Equal(t, len(ch), len(ops))

					var last operation.Operation
					for e := range ch {
						last = e
					}

					assert.Equal(t, ops[len(ops)-1].GetEntry().GetHash().String(), last.GetEntry().GetHash().String())
				})
			})

			t.Run("Options: ranges", func(t *testing.T) {
				t.Run("gt & gte", func(t *testing.T) {
					t.Run("returns 1 item when gte is the head", func(t *testing.T) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{GTE: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &infinity})
						assert.NoError(t, err)

						assert.Equal(t, 1, len(messages))
						assert.Equal(t, ops[len(ops)-1].GetEntry().GetHash().String(), messages[0].GetEntry().GetHash().String())
					})
					t.Run("returns 0 items when gt is the head", func(t *testing.T) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{GT: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &infinity})
						assert.NoError(t, err)

						assert.Equal(t, 0, len(messages))
					})
					t.Run("returns 2 item when gte is defined", func(t *testing.T) {
						gte := ops[len(ops)-2].GetEntry().GetHash()

						messages, err := db.List(ctx, &orbitdb.StreamOptions{GTE: &gte, Amount: &infinity})
						assert.NoError(t, err)

						assert.Equal(t, 2, len(messages))
						assert.Equal(t, ops[len(ops)-2].GetEntry().GetHash().String(), messages[0].GetEntry().GetHash().String())
						assert.Equal(t, ops[len(ops)-1].GetEntry().GetHash().String(), messages[1].GetEntry().GetHash().String())
					})
					t.Run("returns all items when gte is the root item", func(t *testing.T) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{GTE: cidPtr(ops[0].GetEntry().GetHash()), Amount: &infinity})
						assert.NoError(t, err)

						assert.Equal(t, len(ops), len(messages))
						assert.Equal(t, ops[0].GetEntry().GetHash().String(), messages[0].GetEntry().GetHash().String())
						assert.Equal(t, ops[len(ops)-1].GetEntry().GetHash().String(), messages[len(messages)-1].GetEntry().GetHash().String())
					})
					t.Run("returns items when gt is the root item", func(t *testing.T) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{GT: cidPtr(ops[0].GetEntry().GetHash()), Amount: &infinity})
						assert.NoError(t, err)

						assert.Equal(t, len(ops)-1, len(messages))
						assert.Equal(t, ops[1].GetEntry().GetHash().String(), messages[0].GetEntry().GetHash().String())
						assert.Equal(t, ops[len(ops)-1].GetEntry().GetHash().String(), messages[len(messages)-1].GetEntry().GetHash().String())
					})
					t.Run("returns items when gt is defined", func(t *testing.T) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
						assert.NoError(t, err)
						assert.Equal(t, 5, len(messages))

						gt := messages[2].GetEntry().GetHash()
						hundred := 100

						messages2, err := db.List(ctx, &orbitdb.StreamOptions{GT: &gt, Amount: &hundred})
						assert.NoError(t, err)

						assert.Equal(t, 2, len(messages2))
						assert.Equal(t, messages[len(messages)-2].GetEntry().GetHash().String(), messages2[0].GetEntry().GetHash().String())
						assert.Equal(t, messages[len(messages)-1].GetEntry().GetHash().String(), messages2[1].GetEntry().GetHash().String())
					})
				})

				t.Run("lt & lte", func(t *testing.T) {
					t.Run("returns one item after head when lt is the head", func(t *testing.T) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{LT: cidPtr(ops[len(ops)-1].GetEntry().GetHash())})
						assert.NoError(t, err)

						assert.Equal(t, 1, len(messages))
						assert.Equal(t, ops[len(ops)-2].GetEntry().GetHash().String(), messages[0].GetEntry().GetHash().String())
					})
					t.Run("returns all items when lt is head and limit is -1", func(t *testing.T) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{LT: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &infinity})
						assert.NoError(t, err)

						assert.Equal(t, len(ops)-1, len(messages))
						assert.Equal(t, ops[0].GetEntry().GetHash().String(), messages[0].GetEntry().GetHash().String())
						assert.Equal(t, ops[len(ops)-2].GetEntry().GetHash().String(), messages[len(messages)-1].GetEntry().GetHash().String())
					})
					t.Run("returns 3 items when lt is head and limit is 3", func(t *testing.T) {
						three := 3
						messages, err := db.List(ctx, &orbitdb.StreamOptions{LT: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &three})
						assert.NoError(t, err)

						assert.Equal(t, 3, len(messages))
						assert.Equal(t, ops[len(ops)-4].GetEntry().GetHash().String(), messages[0].GetEntry().GetHash().String())
						assert.Equal(t, ops[len(ops)-2].GetEntry().GetHash().String(), messages[2].GetEntry().GetHash().String())
					})
					t.Run("returns null when lt is the root item", func(t *testing.T) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{LT: cidPtr(ops[0].GetEntry().GetHash())})
						assert.NoError(t, err)
						assert.Equal(t, 0, len(messages))
					})
					t.Run("returns one item when lte is the root item", func(t *testing.T) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{LTE: cidPtr(ops[0].GetEntry().GetHash())})
						assert.NoError(t, err)
						assert.Equal(t, 1, len(messages))
						assert.Equal(t, ops[0].GetEntry().GetHash().String(), messages[0].GetEntry().GetHash().String())
					})
					t.Run("returns all items when lte is the head", func(t *testing.T) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{LTE: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &infinity})
						assert.NoError(t, err)
						assert.Equal(t, itemCount, len(messages))
						assert.Equal(t, ops[0].GetEntry().GetHash().String(), messages[0].GetEntry().GetHash().String())
						assert.Equal(t, ops[itemCount-1].GetEntry().GetHash().String(), messages[4].GetEntry().GetHash().String())
					})
					t.Run("returns 3 items when lte is the head", func(t *testing.T) {
						three := 3
						messages, err := db.List(ctx, &orbitdb.StreamOptions{LTE: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &three})
						assert.NoError(t, err)
						assert.Equal(t, three, len(messages))
						assert.Equal(t, ops[itemCount-3].GetEntry().GetHash().String(), messages[0].GetEntry().GetHash().String())
						assert.Equal(t, ops[itemCount-2].GetEntry().GetHash().String(), messages[1].GetEntry().GetHash().String())
						assert.Equal(t, ops[itemCount-1].GetEntry().GetHash().String(), messages[2].GetEntry().GetHash().String())
					})
				})
			})
		})
	})
}
