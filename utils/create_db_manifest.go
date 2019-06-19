package utils

import (
	"berty.tech/go-ipfs-log/io"
	"context"
	"github.com/berty/go-orbit-db/ipfs"
	"github.com/ipfs/go-cid"
	"path"
)

type Manifest struct {
	Name             string
	Type             string
	AccessController string
}

func CreateDBManifest(ctx context.Context, services ipfs.Services, name string, dbType string, accessControllerAddress string, options interface{}) (cid.Cid, error) {
	manifest := &Manifest{
		Name:             name,
		Type:             dbType,
		AccessController: path.Join("/ipfs", accessControllerAddress),
	}

	return io.WriteCBOR(ctx, services, manifest)
}
