package tests

import (
	"context"
	"fmt"
	"github.com/berty/go-orbit-db/orbitdb"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"path"
	"testing"
	"time"
)

func TestLogDatabase(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	const dbPath = "./orbitdb/tests/eventlog"

	//.createInstance(ipfs, { directory: path.join(dbPath, '1') })
	Convey("creates and opens a database", t, FailureHalts, func(c C) {

		err := os.RemoveAll(dbPath)
		c.So(err, ShouldBeNil)

		db1Path := path.Join(dbPath, "1")
		db1IPFS := makeIPFS(ctx, t)

		orbitdb1, err := orbitdb.NewOrbitDB(ctx, db1IPFS, &orbitdb.NewOrbitDBOptions{
			Directory: &db1Path,
		})
		defer orbitdb1.Close()

		c.So(err, ShouldBeNil)


		Convey("creates and opens a database", FailureHalts, func(c C) {
			db, err := orbitdb1.Log(ctx, "log database", nil)
			if err, ok := err.(stackTracer); ok {
				for _, f := range err.StackTrace() {
					fmt.Printf("%+s:%d\n", f, f)
				}
			}
			c.So(err, ShouldBeNil)
			c.So(db, ShouldNotBeNil)

			c.So(db.Type(), ShouldEqual, "eventlog")
			c.So(db.DBName(), ShouldEqual, "log database")
		})
	})
}
