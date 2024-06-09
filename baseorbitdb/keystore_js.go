//go:build js

package baseorbitdb

import (
	"errors"

	"berty.tech/go-ipfs-log/keystore"
	"github.com/libp2p/go-libp2p/core/peer"
)

func NewKeystore(id peer.ID, directory string) (*keystore.Keystore, func() error, error) {
	return nil, nil, errors.New("can't create default keystore in js")
}
