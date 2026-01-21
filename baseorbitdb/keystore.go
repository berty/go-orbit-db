//go:build !js

package baseorbitdb

import (
	"fmt"
	"path"

	"berty.tech/go-ipfs-log/keystore"
	"berty.tech/go-orbit-db/cache/cacheleveldown"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/libp2p/go-libp2p/core/peer"
)

func NewKeystore(id peer.ID, directory string) (*keystore.Keystore, func() error, error) {
	var err error
	var ds *leveldb.Datastore

	// create new datastore
	if directory == cacheleveldown.InMemoryDirectory {
		ds, err = leveldb.NewDatastore("", nil)
	} else {
		ds, err = leveldb.NewDatastore(path.Join(directory, id.String(), "/keystore"), nil)
	}

	if err != nil {
		return nil, nil, fmt.Errorf("unable to create data store used by keystore: %w", err)
	}

	ks, err := keystore.NewKeystore(ds)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create keystore: %w", err)
	}

	return ks, ds.Close, nil
}
