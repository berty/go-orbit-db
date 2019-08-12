// utils contains functions that are useful in some context
package utils

import (
	"berty.tech/go-ipfs-log/io"
	"context"
	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
	"github.com/pkg/errors"
	"github.com/polydawn/refmt/obj/atlas"
	"path"
)

// Manifest defines a database manifest describing its type and access controller
type Manifest struct {
	Name             string
	Type             string
	AccessController string
}

// CreateDBManifest creates a new database manifest and saves it on IPFS
func CreateDBManifest(ctx context.Context, ipfs coreapi.CoreAPI, name string, dbType string, accessControllerAddress string) (cid.Cid, error) {
	manifest := &Manifest{
		Name:             name,
		Type:             dbType,
		AccessController: path.Join("/ipfs", accessControllerAddress),
	}

	c, err := io.WriteCBOR(ctx, ipfs, manifest)
	if err != nil {
		return cid.Cid{}, errors.Wrap(err, "unable to write cbor data")
	}

	return c, err
}

// AtlasManifest defines how a manifest is serialized
var AtlasManifest = atlas.BuildEntry(Manifest{}).
	StructMap().
	AddField("Name", atlas.StructMapEntry{SerialName: "name"}).
	AddField("Type", atlas.StructMapEntry{SerialName: "type"}).
	AddField("AccessController", atlas.StructMapEntry{SerialName: "access_controller"}).
	Complete()

func init() {
	cbornode.RegisterCborType(AtlasManifest)
}
