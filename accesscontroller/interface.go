package accesscontroller

import (
	"context"

	"github.com/stateless-minds/go-ipfs-log/accesscontroller"
	"github.com/stateless-minds/go-ipfs-log/iface"
	"github.com/stateless-minds/go-orbit-db/address"

	"go.uber.org/zap"
)

type LogEntry = iface.IPFSLogEntry
type CanAppendAdditionalContext = accesscontroller.CanAppendAdditionalContext

// Interface The interface for OrbitDB Access Controllers
type Interface interface {
	accesscontroller.Interface

	// Type Returns the type of the store as a string
	Type() string

	Address() address.Address

	// GetAuthorizedByRole Returns the list of keys authorized for a given role
	GetAuthorizedByRole(role string) ([]string, error)

	// Grant Allows a new key for a given role
	Grant(ctx context.Context, capability string, keyID string) error

	// Revoke Removes the permission of a key to perform an action
	Revoke(ctx context.Context, capability string, keyID string) error

	// Load Fetches the configuration of the access controller using the given
	// address
	Load(ctx context.Context, address string) error

	// Save Persists the store configuration (its manifest)
	Save(ctx context.Context) (ManifestParams, error)

	// Close Closes the store
	Close() error

	SetLogger(logger *zap.Logger)
	Logger() *zap.Logger
}

type Option func(ac Interface)
