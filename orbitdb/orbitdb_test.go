package orbitdb_test

import (
	"context"
	"testing"

	"github.com/berty/go-orbit-db/orbitdb"
	ipfsCore "github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/coreapi"
)

func TestNewOrbitDB(t *testing.T) {
	ctx := context.Background()

	core, err := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{})
	if err != nil {
		t.Fatal(err)
	}

	api, err := coreapi.NewCoreAPI(core)
	if err != nil {
		t.Fatal(err)
	}

	db, err := orbitdb.NewOrbitDB(ctx, api, nil)
	if err != nil {
		t.Fatal(err)
	}

	_ = db
}
