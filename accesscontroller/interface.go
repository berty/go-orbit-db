// accesscontroller is a package handling permissions for OrbitDB stores
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
	// Address Returns an access controller address
	Address() address.Address

	// CanAppend Checks whether an entry creator can insert it in the store
	CanAppend(entry *entry.Entry, p idp.Interface) error

	// Type Returns the type of the store as a string
	Type() string

	// GetAuthorizedByRole Returns the list of keys authorized for a given role
	GetAuthorizedByRole(role string) ([]string, error)

	// Grant Allows a key for a given role
	Grant(ctx context.Context, capability string, keyID string) error

	// Revoke Removes the permission of a key to perform an action
	Revoke(ctx context.Context, capability string, keyID string) error

	// Load Fetches the configuration of the access controller using the given
	// address
	Load(ctx context.Context, address string) error

	// Save Persists the store configuration
	Save(ctx context.Context) (ManifestParams, error)

	// Close Closes the store
	Close() error
}
