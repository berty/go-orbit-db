package utils

import (
	"context"
	"fmt"
	"path"

	cid "github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	coreiface "github.com/ipfs/kubo/core/coreiface"
	"github.com/polydawn/refmt/obj/atlas"
	"github.com/stateless-minds/go-ipfs-log/io"
)

type ManifestEnvelope struct {
	Manifest     Manifest    `cbor:"manifest"`
	SkipManifest interface{} `cbor:"skip_manifest,omitempty"`
	Address      interface{} `cbor:"manifest/address,omitempty"`
	ManifestType interface{} `cbor:"manifest/type,omitempty"`

	// // Fields for keys at this level
	Type             string `cbor:"type"`
	Name             string `cbor:"name,omitempty"`
	AccessController string `cbor:"access_controller,omitempty"`
}

// Manifest struct for nodes with "name", "type", and "access_controller"
type Manifest struct {
	SkipManifest interface{} `cbor:"manifest/skip_manifest,omitempty"`
	Address      interface{} `cbor:"manifest/address,omitempty"`
	ManifestType interface{} `cbor:"manifest/type,omitempty"`

	Name             string `cbor:"name"`
	Type             string `cbor:"type"`
	AccessController string `cbor:"access_controller"`
}

// CreateDBManifest creates a new database manifest and saves it on IPFS
func CreateDBManifest(ctx context.Context, ipfs coreiface.CoreAPI, name string, dbType string, accessControllerAddress string) (cid.Cid, error) {
	manifest := &Manifest{
		Name:             name,
		Type:             dbType,
		AccessController: path.Join("/ipfs", accessControllerAddress),
	}

	c, err := io.WriteCBOR(ctx, ipfs, manifest, nil)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("unable to write cbor data: %w", err)
	}

	return c, err
}

// AtlasManifest defines how a manifest is serialized
var AtlasManifest = atlas.BuildEntry(Manifest{}).
	StructMap().
	AddField("SkipManifest", atlas.StructMapEntry{SerialName: "skip_manifest"}).
	AddField("Address", atlas.StructMapEntry{SerialName: "address"}).
	AddField("ManifestType", atlas.StructMapEntry{SerialName: "manifest/type"}).
	AddField("Name", atlas.StructMapEntry{SerialName: "name"}).
	AddField("Type", atlas.StructMapEntry{SerialName: "type"}).
	AddField("AccessController", atlas.StructMapEntry{SerialName: "access_controller"}).
	Complete()

func init() {
	cbornode.RegisterCborType(AtlasManifest)
}
