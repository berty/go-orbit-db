package accesscontroller

import (
	"berty.tech/go-ipfs-log/accesscontroller"
	"berty.tech/go-ipfs-log/iface"
	"context"

	"berty.tech/go-orbit-db/events"
	"go.uber.org/zap"
)

type LogEntry = iface.IPFSLogEntry
type CanAppendAdditionalContext = accesscontroller.CanAppendAdditionalContext

// Interface The interface for OrbitDB Access Controllers
type Interface interface {
	events.EmitterInterface
	accesscontroller.Interface

	// Type Returns the type of the store as a string
	Type() string

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
