package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	orbitdb2 "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores/documentstore"
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

func testingDocsStore(t *testing.T, dir string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbname := "orbit-db-tests"

	t.Run("orbit-db - Documents Database", func(t *testing.T) {
		mocknet := testingMockNet(t)

		node, clean := testingIPFSNode(ctx, t, mocknet)
		defer clean()

		db1IPFS := testingCoreAPI(t, node)

		orbitdb1, err := orbitdb2.NewOrbitDB(ctx, db1IPFS, &orbitdb2.NewOrbitDBOptions{
			Directory: &dir,
		})
		defer func() { _ = orbitdb1.Close() }()

		require.NoError(t, err)

		db, err := orbitdb1.Docs(ctx, dbname, nil)
		require.NoError(t, err)

		defer func() { _ = db.Close() }()

		t.Run("creates and opens a database", func(t *testing.T) {
			db, err := orbitdb1.Docs(ctx, "first docs database", nil)
			require.NoError(t, err)

			if db == nil {
				t.Fatalf("db should not be nil")
			}

			defer func() { _ = db.Close() }()

			require.Equal(t, "docstore", db.Type())
			require.Equal(t, "first docs database", db.DBName())
		})

		document := map[string]interface{}{"_id": "doc1", "hello": "world"}
		documentUpdate1 := map[string]interface{}{"_id": "doc1", "hello": "galaxy"}
		documentUppercase := map[string]interface{}{"_id": "DOCUPPER1", "hello": "world"}

		t.Run("put/get", func(t *testing.T) {
			_, err := db.Put(ctx, document)
			require.NoError(t, err)

			docs, err := db.Get(ctx, "doc1", &iface.DocumentStoreGetOptions{CaseInsensitive: false})
			require.NoError(t, err)
			require.Equal(t, 1, len(docs))
			require.Equal(t, document, docs[0])
		})

		_, err = db.Put(ctx, documentUppercase)
		require.NoError(t, err)

		t.Run("get case insensitive", func(t *testing.T) {
			docs, err := db.Get(ctx, "DOC1", &iface.DocumentStoreGetOptions{CaseInsensitive: true})
			require.NoError(t, err)
			require.Equal(t, 1, len(docs))
			require.Equal(t, document, docs[0])

			docs, err = db.Get(ctx, "docupper1", &iface.DocumentStoreGetOptions{CaseInsensitive: true})
			require.NoError(t, err)
			require.Equal(t, 1, len(docs))
			require.Equal(t, documentUppercase, docs[0])
		})

		t.Run("get case sensitive without match", func(t *testing.T) {
			docs, err := db.Get(ctx, "DOC1", &iface.DocumentStoreGetOptions{CaseInsensitive: false})
			require.NoError(t, err)
			require.Equal(t, 0, len(docs))

			docs, err = db.Get(ctx, "docupper1", &iface.DocumentStoreGetOptions{CaseInsensitive: false})
			require.NoError(t, err)
			require.Equal(t, 0, len(docs))
		})

		t.Run("put updates a value", func(t *testing.T) {
			_, err = db.Put(ctx, documentUpdate1)
			require.NoError(t, err)

			docs, err := db.Get(ctx, "doc1", &iface.DocumentStoreGetOptions{CaseInsensitive: false})
			require.NoError(t, err)
			require.Equal(t, 1, len(docs))
			require.Equal(t, documentUpdate1, docs[0])
		})

		t.Run("put/get - multiple keys", func(t *testing.T) {
			documentOne := map[string]interface{}{"_id": "doc1", "hello": "world"}
			_, err := db.Put(ctx, documentOne)
			require.NoError(t, err)

			documentTwo := map[string]interface{}{"_id": "doc2", "hello": "galaxy"}
			_, err = db.Put(ctx, documentTwo)
			require.NoError(t, err)

			documentThree := map[string]interface{}{"_id": "doc3", "hello": "universe"}
			_, err = db.Put(ctx, documentThree)
			require.NoError(t, err)

			docsOne, err := db.Get(ctx, "doc1", &iface.DocumentStoreGetOptions{CaseInsensitive: false})
			require.NoError(t, err)

			docsTwo, err := db.Get(ctx, "doc2", &iface.DocumentStoreGetOptions{CaseInsensitive: false})
			require.NoError(t, err)

			docsThree, err := db.Get(ctx, "doc3", &iface.DocumentStoreGetOptions{CaseInsensitive: false})
			require.NoError(t, err)

			require.Equal(t, 1, len(docsOne))
			require.Equal(t, documentOne, docsOne[0])
			require.Equal(t, 1, len(docsTwo))
			require.Equal(t, documentTwo, docsTwo[0])
			require.Equal(t, 1, len(docsThree))
			require.Equal(t, documentThree, docsThree[0])
		})

		t.Run("get - partial term match - PartialMatches: true", func(t *testing.T) {
			doc1 := map[string]interface{}{"_id": "hello world", "doc": "some things"}
			doc2 := map[string]interface{}{"_id": "hello universe", "doc": "all the things"}
			doc3 := map[string]interface{}{"_id": "sup world", "doc": "other things"}

			_, err := db.Put(ctx, doc1)
			require.NoError(t, err)

			_, err = db.Put(ctx, doc2)
			require.NoError(t, err)

			_, err = db.Put(ctx, doc3)
			require.NoError(t, err)

			fetchedDocs, err := db.Get(ctx, "hello", &iface.DocumentStoreGetOptions{PartialMatches: true})
			require.NoError(t, err)
			require.Equal(t, 2, len(fetchedDocs))
			require.Contains(t, fetchedDocs, doc1)
			require.Contains(t, fetchedDocs, doc2)
		})

		t.Run("get - partial term match - PartialMatches: false", func(t *testing.T) {
			doc1 := map[string]interface{}{"_id": "hello world", "doc": "some things"}
			doc2 := map[string]interface{}{"_id": "hello universe", "doc": "all the things"}
			doc3 := map[string]interface{}{"_id": "sup world", "doc": "other things"}

			_, err := db.Put(ctx, doc1)
			require.NoError(t, err)

			_, err = db.Put(ctx, doc2)
			require.NoError(t, err)

			_, err = db.Put(ctx, doc3)
			require.NoError(t, err)

			fetchedDocs, err := db.Get(ctx, "hello", &iface.DocumentStoreGetOptions{PartialMatches: false})
			require.NoError(t, err)

			require.Equal(t, 0, len(fetchedDocs))
		})

		t.Run("deletes a key", func(t *testing.T) {
			document := map[string]interface{}{"_id": "doc1", "hello": "world"}
			_, err := db.Put(ctx, document)
			require.NoError(t, err)

			_, err = db.Delete(ctx, "doc1")
			require.NoError(t, err)

			docs, err := db.Get(ctx, "doc1", &iface.DocumentStoreGetOptions{CaseInsensitive: false})
			require.NoError(t, err)
			require.Equal(t, 0, len(docs))
		})

		t.Run("deletes a key after multiple updates", func(t *testing.T) {
			documentOne := map[string]interface{}{"_id": "doc1", "hello": "world"}
			_, err := db.Put(ctx, documentOne)
			require.NoError(t, err)

			documentTwo := map[string]interface{}{"_id": "doc1", "hello": "galaxy"}
			_, err = db.Put(ctx, documentTwo)
			require.NoError(t, err)

			documentThree := map[string]interface{}{"_id": "doc1", "hello": "universe"}
			_, err = db.Put(ctx, documentThree)
			require.NoError(t, err)

			_, err = db.Delete(ctx, "doc1")
			require.NoError(t, err)

			docs, err := db.Get(ctx, "doc1", &iface.DocumentStoreGetOptions{CaseInsensitive: false})
			require.NoError(t, err)
			require.Equal(t, 0, len(docs))
		})

		t.Run("Specified index", func(t *testing.T) {
			options := documentstore.DefaultStoreOptsForMap("doc")

			db, err := orbitdb1.Docs(ctx, "orbit-db-tests-specified-index", &iface.CreateDBOptions{StoreSpecificOpts: options})
			require.NoError(t, err)

			defer func() { _ = db.Drop() }()

			doc1 := map[string]interface{}{"_id": "hello world", "doc": "all the things"}
			doc2 := map[string]interface{}{"_id": "hello world", "doc": "some things"}

			t.Run("put", func(t *testing.T) {
				_, err := db.Put(ctx, doc1)
				require.NoError(t, err)

				value, err := db.Get(ctx, "all", &iface.DocumentStoreGetOptions{PartialMatches: true})
				require.NoError(t, err)

				require.Equal(t, 1, len(value))
				require.Equal(t, doc1, value[0])
			})

			t.Run("matches specified index", func(t *testing.T) {
				_, err = db.Put(ctx, doc2)
				require.NoError(t, err)

				value1, err := db.Get(ctx, "all", &iface.DocumentStoreGetOptions{PartialMatches: true})
				require.NoError(t, err)

				require.Equal(t, 1, len(value1))
				require.Equal(t, doc1, value1[0])

				value2, err := db.Get(ctx, "some", &iface.DocumentStoreGetOptions{PartialMatches: true})
				require.NoError(t, err)

				require.Equal(t, 1, len(value2))
				require.Equal(t, doc2, value2[0])
			})
		})

		t.Run("putAll", func(t *testing.T) {
			db, err := orbitdb1.Docs(ctx, "orbit-db-tests-putall", nil)
			require.NoError(t, err)

			defer func() { _ = db.Drop() }()

			doc1 := map[string]interface{}{"_id": "id1", "doc": "all the things"}
			doc2 := map[string]interface{}{"_id": "id2", "doc": "some things"}
			doc3 := map[string]interface{}{"_id": "id3", "doc": "more things"}

			_, err = db.PutAll(ctx, []interface{}{doc1, doc2, doc3})
			require.NoError(t, err)

			value, err := db.Get(ctx, "", &iface.DocumentStoreGetOptions{PartialMatches: true})
			require.NoError(t, err)

			require.Equal(t, 3, len(value))
			require.Contains(t, value, doc1)
			require.Contains(t, value, doc2)
			require.Contains(t, value, doc3)
		})

		t.Run("query", func(t *testing.T) {
			viewsFilter := func(expectedCount int) func(e interface{}) (bool, error) {
				return func(e interface{}) (bool, error) {
					entry, ok := e.(map[string]interface{})
					if !ok {
						return false, fmt.Errorf("unable to cast entry")
					}

					if _, ok := entry["views"]; !ok {
						return false, nil
					}

					views, ok := entry["views"].(float64)
					if !ok {
						return false, fmt.Errorf("unable to cast value for field views")
					}

					return int(views) > expectedCount, nil
				}
			}

			t.Run("query - simple", func(t *testing.T) {
				db, err := orbitdb1.Docs(ctx, "orbit-db-tests-putall", nil)
				require.NoError(t, err)

				defer func() { _ = db.Drop() }()

				doc1 := map[string]interface{}{"_id": "hello world", "doc": "all the things", "views": 17}
				doc2 := map[string]interface{}{"_id": "sup world', doc: 'some of the things", "views": 10}
				doc3 := map[string]interface{}{"_id": "hello other world", "doc": "none of the things", "views": 5}
				doc4 := map[string]interface{}{"_id": "hey universe", "doc": ""}

				for _, doc := range []map[string]interface{}{doc1, doc2, doc3, doc4} {
					_, err := db.Put(ctx, doc)
					require.NoError(t, err)
				}

				value, err := db.Query(ctx, viewsFilter(5))
				require.NoError(t, err)
				require.Len(t, value, 2)
				require.Contains(t, fmt.Sprintf("%v", value), fmt.Sprintf("%v", doc1))
				require.Contains(t, fmt.Sprintf("%v", value), fmt.Sprintf("%v", doc2))

				value, err = db.Query(ctx, viewsFilter(10))
				require.NoError(t, err)
				require.Len(t, value, 1)
				require.Contains(t, fmt.Sprintf("%v", value), fmt.Sprintf("%v", doc1))

				value, err = db.Query(ctx, viewsFilter(17))
				require.NoError(t, err)
				require.Len(t, value, 0)
			})

			t.Run("query after delete", func(t *testing.T) {
				db, err := orbitdb1.Docs(ctx, "orbit-db-tests-putall", nil)
				require.NoError(t, err)

				defer func() { _ = db.Drop() }()

				doc1 := map[string]interface{}{"_id": "hello world", "doc": "all the things", "views": 17}
				doc2 := map[string]interface{}{"_id": "sup world', doc: 'some of the things", "views": 10}
				doc3 := map[string]interface{}{"_id": "hello other world", "doc": "none of the things", "views": 5}
				doc4 := map[string]interface{}{"_id": "hey universe", "doc": ""}

				for _, doc := range []map[string]interface{}{doc1, doc2, doc3, doc4} {
					_, err := db.Put(ctx, doc)
					require.NoError(t, err)
				}
				_, err = db.Delete(ctx, "hello world")
				require.NoError(t, err)

				value, err := db.Query(ctx, viewsFilter(4))
				require.NoError(t, err)
				require.Len(t, value, 2)
				require.Contains(t, fmt.Sprintf("%v", value), fmt.Sprintf("%v", doc2))
				require.Contains(t, fmt.Sprintf("%v", value), fmt.Sprintf("%v", doc3))

				value, err = db.Query(ctx, viewsFilter(9))
				require.NoError(t, err)
				require.Len(t, value, 1)
				require.Contains(t, fmt.Sprintf("%v", value), fmt.Sprintf("%v", doc2))
			})
		})
	})
}
