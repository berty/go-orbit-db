package accesscontroller

import (
	"berty.tech/go-ipfs-log/entry"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"context"
	"github.com/berty/go-orbit-db/address"
)

type SimpleInterface interface {
	CanAppend(entry *entry.Entry, p idp.Interface) error
}

type Interface interface {
	SimpleInterface
	Type() string
	Address() address.Address
	Grant(ctx context.Context, capability string, keyID string) error
	Revoke(ctx context.Context, capability string, keyID string) error
	Load(ctx context.Context, address string) error
	Save(ctx context.Context) (ManifestParams, error)
	Close() error
}
