package accesscontroller

import (
	"berty.tech/go-ipfs-log/entry"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"context"
	"github.com/berty/go-orbit-db/address"
	"github.com/berty/go-orbit-db/events"
)

type Interface interface {
	events.EmitterInterface
	Address() address.Address
	CanAppend(entry *entry.Entry, p idp.Interface) error
	Type() string
	GetAuthorizedByRole(role string) ([]string, error)
	Revoke(ctx context.Context, capability string, keyID string) error
	Load(ctx context.Context, address string) error
	Save(ctx context.Context) (ManifestParams, error)
	Close() error
}
