package tests

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/stores/operation"
	. "github.com/smartystreets/goconvey/convey"
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
	Convey("creates and opens a database", t, FailureHalts, func(c C) {
		mocknet := testingMockNet(ctx)
		node, clean := testingIPFSNode(ctx, t, mocknet)
		defer clean()

		db1IPFS := testingCoreAPI(t, node)

		infinity := -1

		orbitdb1, err := orbitdb.NewOrbitDB(ctx, db1IPFS, &orbitdb.NewOrbitDBOptions{
			Directory: &db1Path,
		})
		c.So(err, ShouldBeNil)

		defer orbitdb1.Close()

		c.Convey("basic tests", FailureHalts, func(c C) {
			db, err := orbitdb1.Log(ctx, "log database", nil)
			c.So(err, ShouldBeNil)
			if db == nil {
				t.Fatalf("db should not be nil")
			}

			////// creates and opens a database
			c.So(db.Type(), ShouldEqual, "eventlog")
			c.So(db.DBName(), ShouldEqual, "log database")

			////// returns 0 items when it's a fresh database
			res := make(chan operation.Operation, 100)
			err = db.Stream(ctx, res, &orbitdb.StreamOptions{Amount: &infinity})
			c.So(err, ShouldBeNil)
			c.So(len(res), ShouldEqual, 0)

			////// returns the added entry's hash, 1 entry
			op, err := db.Add(ctx, []byte("hello1"))
			c.So(err, ShouldBeNil)

			ops, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})

			c.So(err, ShouldBeNil)
			c.So(len(ops), ShouldEqual, 1)
			item := ops[0]

			c.So(item.GetEntry().GetHash().String(), ShouldEqual, op.GetEntry().GetHash().String())

			////// returns the added entry's hash, 2 entries
			err = db.Load(ctx, -1)
			c.So(err, ShouldBeNil)

			ops, err = db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
			c.So(err, ShouldBeNil)
			c.So(len(ops), ShouldEqual, 1)

			prevHash := ops[0].GetEntry().GetHash()

			op, err = db.Add(ctx, []byte("hello2"))
			c.So(err, ShouldBeNil)

			ops, err = db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
			c.So(err, ShouldBeNil)
			c.So(len(ops), ShouldEqual, 2)

			c.So(ops[1].GetEntry().GetHash().String(), ShouldNotEqual, prevHash.String())
			c.So(ops[1].GetEntry().GetHash().String(), ShouldEqual, op.GetEntry().GetHash().String())
		})

		c.Convey("adds five items", FailureHalts, func(c C) {
			db, err := orbitdb1.Log(ctx, "second database", nil)
			c.So(err, ShouldBeNil)

			for i := 1; i <= 5; i++ {
				_, err := db.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
				c.So(err, ShouldBeNil)
			}

			items, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
			c.So(err, ShouldBeNil)
			c.So(len(items), ShouldEqual, 5)

			for i := 1; i <= 5; i++ {
				c.So(string(items[i-1].GetValue()), ShouldEqual, fmt.Sprintf("hello%d", i))
			}
		})

		c.Convey("adds an item that is > 256 bytes", FailureHalts, func(c C) {
			db, err := orbitdb1.Log(ctx, "third database", nil)
			c.So(err, ShouldBeNil)

			msg := bytes.Repeat([]byte("a"), 1024)

			op, err := db.Add(ctx, msg)
			c.So(err, ShouldBeNil)
			c.So(op.GetEntry().GetHash().String(), ShouldStartWith, "bafy")
		})

		c.Convey("iterator & collect & options", FailureHalts, func(c C) {
			itemCount := 5
			var ops []operation.Operation

			db, err := orbitdb1.Log(ctx, "iterator tests", nil)
			c.So(err, ShouldBeNil)

			for i := 0; i < itemCount; i++ {
				op, err := db.Add(ctx, []byte(fmt.Sprintf("hello%d", i)))
				c.So(err, ShouldBeNil)
				ops = append(ops, op)
			}

			c.Convey("iterator", FailureHalts, func(c C) {
				c.Convey("defaults", FailureHalts, func(c C) {
					c.Convey("returns an item with the correct structure", FailureHalts, func(c C) {
						ch := make(chan operation.Operation, 100)

						err = db.Stream(ctx, ch, nil)
						c.So(err, ShouldBeNil)

						next := <-ch

						c.So(next, ShouldNotBeNil)
						c.So(next.GetEntry().GetHash().String(), ShouldStartWith, "bafy")
						c.So(next.GetKey(), ShouldBeNil)
						c.So(string(next.GetValue()), ShouldEqual, "hello4")
					})

					c.Convey("implements Iterator interface", FailureHalts, func(c C) {
						ch := make(chan operation.Operation, 100)

						err = db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &infinity})
						c.So(err, ShouldBeNil)

						c.So(len(ch), ShouldEqual, itemCount)
					})

					c.Convey("returns 1 item as default", FailureHalts, func(c C) {
						ch := make(chan operation.Operation, 100)

						err = db.Stream(ctx, ch, nil)
						c.So(err, ShouldBeNil)

						first := <-ch
						second := <-ch

						c.So(first.GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-1].GetEntry().GetHash().String())
						c.So(second, ShouldEqual, nil)
						c.So(string(first.GetValue()), ShouldEqual, "hello4")
					})

					c.Convey("returns items in the correct order", FailureHalts, func(c C) {
						ch := make(chan operation.Operation, 100)

						amount := 3

						err := db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &amount})
						c.So(err, ShouldBeNil)

						i := len(ops) - amount

						for op := range ch {
							c.So(string(op.GetValue()), ShouldEqual, fmt.Sprintf("hello%d", i))
							i++
						}
					})
				})
			})

			c.Convey("collect", FailureHalts, func(c C) {
				c.Convey("returns all items", FailureHalts, func(c C) {
					messages, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})

					c.So(err, ShouldBeNil)
					c.So(len(messages), ShouldEqual, len(ops))
					c.So(string(messages[0].GetValue()), ShouldEqual, "hello0")
					c.So(string(messages[len(messages)-1].GetValue()), ShouldEqual, "hello4")
				})

				c.Convey("returns 1 item", FailureHalts, func(c C) {
					messages, err := db.List(ctx, nil)

					c.So(err, ShouldBeNil)
					c.So(len(messages), ShouldEqual, 1)
				})

				c.Convey("returns 3 items", FailureHalts, func(c C) {
					three := 3
					messages, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &three})

					c.So(err, ShouldBeNil)
					c.So(len(messages), ShouldEqual, 3)
				})
			})

			c.Convey("Options: limit", FailureHalts, func(c C) {
				c.Convey("returns 1 item when limit is 0", FailureHalts, func(c C) {
					ch := make(chan operation.Operation, 100)
					zero := 0
					err = db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &zero})
					c.So(err, ShouldBeNil)

					c.So(len(ch), ShouldEqual, 1)

					first := <-ch
					second := <-ch

					c.So(first.GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-1].GetEntry().GetHash().String())
					c.So(second, ShouldBeNil)
				})

				c.Convey("returns 1 item when limit is 1", FailureHalts, func(c C) {
					ch := make(chan operation.Operation, 100)
					one := 1
					err = db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &one})
					c.So(err, ShouldBeNil)

					c.So(len(ch), ShouldEqual, 1)

					first := <-ch
					second := <-ch

					c.So(first.GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-1].GetEntry().GetHash().String())
					c.So(second, ShouldBeNil)
				})

				c.Convey("returns 3 items", FailureHalts, func(c C) {
					ch := make(chan operation.Operation, 100)
					three := 3
					err = db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &three})
					c.So(err, ShouldBeNil)

					c.So(len(ch), ShouldEqual, 3)

					first := <-ch
					second := <-ch
					third := <-ch
					fourth := <-ch

					c.So(first.GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-3].GetEntry().GetHash().String())
					c.So(second.GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-2].GetEntry().GetHash().String())
					c.So(third.GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-1].GetEntry().GetHash().String())
					c.So(fourth, ShouldBeNil)
				})

				c.Convey("returns all items", FailureHalts, func(c C) {
					ch := make(chan operation.Operation, 100)
					err = db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &infinity})
					c.So(err, ShouldBeNil)

					c.So(len(ops), ShouldEqual, len(ch))

					var last operation.Operation
					for e := range ch {
						last = e
					}

					c.So(last.GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-1].GetEntry().GetHash().String())
				})

				c.Convey("returns all items when limit is bigger than -1", FailureHalts, func(c C) {
					ch := make(chan operation.Operation, 100)
					minusThreeHundred := -300
					err = db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &minusThreeHundred})
					c.So(err, ShouldBeNil)

					c.So(len(ops), ShouldEqual, len(ch))

					var last operation.Operation
					for e := range ch {
						last = e
					}

					c.So(last.GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-1].GetEntry().GetHash().String())
				})

				c.Convey("returns all items when limit is bigger than number of items", FailureHalts, func(c C) {
					ch := make(chan operation.Operation, 100)
					threeHundred := 300
					err = db.Stream(ctx, ch, &orbitdb.StreamOptions{Amount: &threeHundred})
					c.So(err, ShouldBeNil)

					c.So(len(ops), ShouldEqual, len(ch))

					var last operation.Operation
					for e := range ch {
						last = e
					}

					c.So(last.GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-1].GetEntry().GetHash().String())
				})
			})

			c.Convey("Options: ranges", FailureHalts, func(c C) {
				c.Convey("gt & gte", FailureHalts, func(c C) {
					c.Convey("returns 1 item when gte is the head", FailureHalts, func(c C) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{GTE: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &infinity})
						c.So(err, ShouldBeNil)

						c.So(len(messages), ShouldEqual, 1)
						c.So(messages[0].GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-1].GetEntry().GetHash().String())
					})
					c.Convey("returns 0 items when gt is the head", FailureHalts, func(c C) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{GT: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &infinity})
						c.So(err, ShouldBeNil)

						c.So(len(messages), ShouldEqual, 0)
					})
					c.Convey("returns 2 item when gte is defined", FailureHalts, func(c C) {
						gte := ops[len(ops)-2].GetEntry().GetHash()

						messages, err := db.List(ctx, &orbitdb.StreamOptions{GTE: &gte, Amount: &infinity})
						c.So(err, ShouldBeNil)

						c.So(len(messages), ShouldEqual, 2)
						c.So(messages[0].GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-2].GetEntry().GetHash().String())
						c.So(messages[1].GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-1].GetEntry().GetHash().String())
					})
					c.Convey("returns all items when gte is the root item", FailureHalts, func(c C) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{GTE: cidPtr(ops[0].GetEntry().GetHash()), Amount: &infinity})
						c.So(err, ShouldBeNil)

						c.So(len(messages), ShouldEqual, len(ops))
						c.So(messages[0].GetEntry().GetHash().String(), ShouldEqual, ops[0].GetEntry().GetHash().String())
						c.So(messages[len(messages)-1].GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-1].GetEntry().GetHash().String())
					})
					c.Convey("returns items when gt is the root item", FailureHalts, func(c C) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{GT: cidPtr(ops[0].GetEntry().GetHash()), Amount: &infinity})
						c.So(err, ShouldBeNil)

						c.So(len(messages), ShouldEqual, len(ops)-1)
						c.So(messages[0].GetEntry().GetHash().String(), ShouldEqual, ops[1].GetEntry().GetHash().String())
						c.So(messages[len(messages)-1].GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-1].GetEntry().GetHash().String())
					})
					c.Convey("returns items when gt is defined", FailureHalts, func(c C) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
						c.So(err, ShouldBeNil)
						c.So(len(messages), ShouldEqual, 5)

						gt := messages[2].GetEntry().GetHash()
						hundred := 100

						messages2, err := db.List(ctx, &orbitdb.StreamOptions{GT: &gt, Amount: &hundred})
						c.So(err, ShouldBeNil)

						c.So(len(messages2), ShouldEqual, 2)
						c.So(messages2[0].GetEntry().GetHash().String(), ShouldEqual, messages[len(messages)-2].GetEntry().GetHash().String())
						c.So(messages2[1].GetEntry().GetHash().String(), ShouldEqual, messages[len(messages)-1].GetEntry().GetHash().String())
					})
				})

				c.Convey("lt & lte", FailureHalts, func(c C) {
					c.Convey("returns one item after head when lt is the head", FailureHalts, func(c C) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{LT: cidPtr(ops[len(ops)-1].GetEntry().GetHash())})
						c.So(err, ShouldBeNil)

						c.So(len(messages), ShouldEqual, 1)
						c.So(messages[0].GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-2].GetEntry().GetHash().String())
					})
					c.Convey("returns all items when lt is head and limit is -1", FailureHalts, func(c C) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{LT: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &infinity})
						c.So(err, ShouldBeNil)

						c.So(len(messages), ShouldEqual, len(ops)-1)
						c.So(messages[0].GetEntry().GetHash().String(), ShouldEqual, ops[0].GetEntry().GetHash().String())
						c.So(messages[len(messages)-1].GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-2].GetEntry().GetHash().String())
					})
					c.Convey("returns 3 items when lt is head and limit is 3", FailureHalts, func(c C) {
						three := 3
						messages, err := db.List(ctx, &orbitdb.StreamOptions{LT: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &three})
						c.So(err, ShouldBeNil)

						c.So(len(messages), ShouldEqual, 3)
						c.So(messages[0].GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-4].GetEntry().GetHash().String())
						c.So(messages[2].GetEntry().GetHash().String(), ShouldEqual, ops[len(ops)-2].GetEntry().GetHash().String())
					})
					c.Convey("returns null when lt is the root item", FailureHalts, func(c C) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{LT: cidPtr(ops[0].GetEntry().GetHash())})
						c.So(err, ShouldBeNil)
						c.So(len(messages), ShouldEqual, 0)
					})
					c.Convey("returns one item when lte is the root item", FailureHalts, func(c C) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{LTE: cidPtr(ops[0].GetEntry().GetHash())})
						c.So(err, ShouldBeNil)
						c.So(len(messages), ShouldEqual, 1)
						c.So(messages[0].GetEntry().GetHash().String(), ShouldEqual, ops[0].GetEntry().GetHash().String())
					})
					c.Convey("returns all items when lte is the head", FailureHalts, func(c C) {
						messages, err := db.List(ctx, &orbitdb.StreamOptions{LTE: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &infinity})
						c.So(err, ShouldBeNil)
						c.So(len(messages), ShouldEqual, itemCount)
						c.So(messages[0].GetEntry().GetHash().String(), ShouldEqual, ops[0].GetEntry().GetHash().String())
						c.So(messages[4].GetEntry().GetHash().String(), ShouldEqual, ops[itemCount-1].GetEntry().GetHash().String())
					})
					c.Convey("returns 3 items when lte is the head", FailureHalts, func(c C) {
						three := 3
						messages, err := db.List(ctx, &orbitdb.StreamOptions{LTE: cidPtr(ops[len(ops)-1].GetEntry().GetHash()), Amount: &three})
						c.So(err, ShouldBeNil)
						c.So(len(messages), ShouldEqual, three)
						c.So(messages[0].GetEntry().GetHash().String(), ShouldEqual, ops[itemCount-3].GetEntry().GetHash().String())
						c.So(messages[1].GetEntry().GetHash().String(), ShouldEqual, ops[itemCount-2].GetEntry().GetHash().String())
						c.So(messages[2].GetEntry().GetHash().String(), ShouldEqual, ops[itemCount-1].GetEntry().GetHash().String())
					})
				})
			})
		})
	})
}
