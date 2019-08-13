package simple

import (
	"context"

	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/accesscontroller/base"
	"berty.tech/go-orbit-db/address"
	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/iface"
	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
)

type simpleAccessController struct {
	events.EventEmitter
	allowedKeys map[string][]string
}

func (o *simpleAccessController) Address() address.Address {
	return nil
}

func (o *simpleAccessController) Grant(ctx context.Context, capability string, keyID string) error {
	return nil
}

func (o *simpleAccessController) Revoke(ctx context.Context, capability string, keyID string) error {
	return nil
}

func (o *simpleAccessController) Load(ctx context.Context, address string) error {
	return nil
}

func (o *simpleAccessController) Save(ctx context.Context) (accesscontroller.ManifestParams, error) {
	return accesscontroller.NewManifestParams(cid.Cid{}, true, "simple"), nil
}

func (o *simpleAccessController) Close() error {
	return nil
}

func (o *simpleAccessController) Type() string {
	return "simple"
}

func (o *simpleAccessController) GetAuthorizedByRole(role string) ([]string, error) {
	return o.allowedKeys[role], nil
}

func (o *simpleAccessController) CanAppend(e *entry.Entry, p identityprovider.Interface) error {
	for _, id := range o.allowedKeys["write"] {
		if e.Identity.ID == id || id == "*" {
			return nil
		}
	}

	return errors.New("not allowed to write entry")
}

// NewSimpleAccessController Returns a non configurable access controller
func NewSimpleAccessController(_ context.Context, _ iface.OrbitDB, options *base.CreateAccessControllerOptions) (accesscontroller.Interface, error) {
	if options.Access == nil {
		options.Access = map[string][]string{}
	}

	return &simpleAccessController{
		allowedKeys: options.Access,
	}, nil
}

var _ accesscontroller.Interface = &simpleAccessController{}

func init() {
	_ = base.AddAccessController(NewSimpleAccessController)
}
