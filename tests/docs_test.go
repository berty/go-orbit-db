package tests

import (
	"context"
	"testing"

	orbitdb2 "berty.tech/go-orbit-db"
	. "github.com/smartystreets/goconvey/convey"
)

func TestDocumentsStore(t *testing.T) {
	tmpDir, clean := testingTempDir(t, "db-docsstore")
	defer clean()

	cases := []struct{ Name, Directory string }{
		{Name: "in memory", Directory: ":memory:"},
		{Name: "persistent", Directory: tmpDir},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			testingDocsStore(t, c.Directory)
		})
	}
}

func createDocument(id string, key string, value string) map[string]interface{} {
	m := make(map[string]interface{})
	m["_id"] = id
	m[key] = value
	return m
}

func testingDocsStore(t *testing.T, dir string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbname := "orbit-db-tests"
	Convey("orbit-db - Documents Database", t, FailureHalts, func(c C) {
		mocknet := testingMockNet(ctx)

		node, clean := testingIPFSNode(ctx, t, mocknet)
		defer clean()

		db1IPFS := testingCoreAPI(t, node)

		orbitdb1, err := orbitdb2.NewOrbitDB(ctx, db1IPFS, &orbitdb2.NewOrbitDBOptions{
			Directory: &dir,
		})
		defer orbitdb1.Close()

		c.So(err, ShouldBeNil)

		db, err := orbitdb1.Docs(ctx, dbname, nil)
		c.So(err, ShouldBeNil)

		defer db.Close()

		c.Convey("creates and opens a database", FailureHalts, func(c C) {
			db, err := orbitdb1.Docs(ctx, "first docs database", nil)
			c.So(err, ShouldBeNil)

			if db == nil {
				t.Fatalf("db should not be nil")
			}

			defer db.Close()

			c.So(db.Type(), ShouldEqual, "docstore")
			c.So(db.DBName(), ShouldEqual, "first docs database")
		})

		c.Convey("put", FailureHalts, func(c C) {
			document := createDocument("doc1", "hello", "world")
			_, err := db.Put(ctx, document)
			c.So(err, ShouldBeNil)

			docs, err := db.Get(ctx, "doc1", true)
			c.So(err, ShouldBeNil)
			c.So(len(docs), ShouldEqual, 1)
			c.So(docs[0], ShouldResemble, document)
		})

		c.Convey("get", FailureHalts, func(c C) {
			document := createDocument("doc1", "hello", "world")
			_, err := db.Put(ctx, document)
			c.So(err, ShouldBeNil)

			docs, err := db.Get(ctx, "doc1", true)
			c.So(err, ShouldBeNil)
			c.So(len(docs), ShouldEqual, 1)
			c.So(docs[0], ShouldResemble, document)
		})

		c.Convey("get case insensitive", FailureHalts, func(c C) {
			document := createDocument("DOC1", "hello", "world")
			_, err := db.Put(ctx, document)
			c.So(err, ShouldBeNil)

			docs, err := db.Get(ctx, "doc1", false)
			c.So(err, ShouldBeNil)
			c.So(len(docs), ShouldEqual, 1)
			c.So(docs[0], ShouldResemble, document)
		})

		c.Convey("get case sensitive without match", FailureHalts, func(c C) {
			document := createDocument("DOC1", "hello", "world")
			_, err := db.Put(ctx, document)
			c.So(err, ShouldBeNil)

			docs, err := db.Get(ctx, "doc1", true)
			c.So(err, ShouldBeNil)
			c.So(len(docs), ShouldEqual, 0)
		})

		c.Convey("put updates a value", FailureHalts, func(c C) {
			documentOne := createDocument("doc1", "hello", "world")
			_, err := db.Put(ctx, documentOne)
			c.So(err, ShouldBeNil)

			documentTwo := createDocument("doc1", "hello", "galaxy")
			_, err = db.Put(ctx, documentTwo)
			c.So(err, ShouldBeNil)

			docs, err := db.Get(ctx, "doc1", true)
			c.So(err, ShouldBeNil)
			c.So(len(docs), ShouldEqual, 1)
			c.So(docs[0], ShouldResemble, documentTwo)
		})

		c.Convey("put/get - multiple keys", FailureHalts, func(c C) {
			documentOne := createDocument("doc1", "hello", "world")
			_, err := db.Put(ctx, documentOne)
			c.So(err, ShouldBeNil)

			documentTwo := createDocument("doc2", "hello", "galaxy")
			_, err = db.Put(ctx, documentTwo)
			c.So(err, ShouldBeNil)

			documentThree := createDocument("doc3", "hello", "universe")
			_, err = db.Put(ctx, documentThree)
			c.So(err, ShouldBeNil)

			docsOne, err := db.Get(ctx, "doc1", true)
			c.So(err, ShouldBeNil)

			docsTwo, err := db.Get(ctx, "doc2", true)
			c.So(err, ShouldBeNil)

			docsThree, err := db.Get(ctx, "doc3", true)
			c.So(err, ShouldBeNil)

			c.So(len(docsOne), ShouldEqual, 1)
			c.So(docsOne[0], ShouldResemble, documentOne)
			c.So(len(docsTwo), ShouldEqual, 1)
			c.So(docsTwo[0], ShouldResemble, documentTwo)
			c.So(len(docsThree), ShouldEqual, 1)
			c.So(docsThree[0], ShouldResemble, documentThree)
		})

		c.Convey("deletes a key", FailureHalts, func(c C) {
			document := createDocument("doc1", "hello", "world")
			_, err := db.Put(ctx, document)
			c.So(err, ShouldBeNil)

			_, err = db.Delete(ctx, "doc1")
			c.So(err, ShouldBeNil)

			docs, err := db.Get(ctx, "doc1", true)
			c.So(err, ShouldBeNil)
			c.So(len(docs), ShouldEqual, 0)
		})

		c.Convey("deletes a key after multiple updates", FailureHalts, func(c C) {
			documentOne := createDocument("doc1", "hello", "world")
			_, err := db.Put(ctx, documentOne)
			c.So(err, ShouldBeNil)

			documentTwo := createDocument("doc1", "hello", "galaxy")
			_, err = db.Put(ctx, documentTwo)
			c.So(err, ShouldBeNil)

			documentThree := createDocument("doc1", "hello", "universe")
			_, err = db.Put(ctx, documentThree)
			c.So(err, ShouldBeNil)

			_, err = db.Delete(ctx, "doc1")
			c.So(err, ShouldBeNil)

			docs, err := db.Get(ctx, "doc1", true)
			c.So(err, ShouldBeNil)
			c.So(len(docs), ShouldEqual, 0)
		})

	})
}
