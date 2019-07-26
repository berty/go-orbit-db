package orbitdb

import (
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/identityprovider"
	"context"
	"encoding/json"
	orbitdb "github.com/berty/go-orbit-db"
	"github.com/berty/go-orbit-db/accesscontroller"
	"github.com/berty/go-orbit-db/accesscontroller/base"
	"github.com/berty/go-orbit-db/accesscontroller/ipfs"
	"github.com/berty/go-orbit-db/accesscontroller/utils"
	"github.com/berty/go-orbit-db/address"
	"github.com/berty/go-orbit-db/events"
	"github.com/berty/go-orbit-db/stores"
	"github.com/berty/go-orbit-db/stores/kvstore"
	"github.com/pkg/errors"
)

type Event interface{}
type EventUpdated struct{}

type orbitDBAccessController struct {
	events.EventEmitter
	orbitdb    orbitdb.OrbitDB
	kvStore    kvstore.OrbitDBKeyValue
	options    *base.CreateAccessControllerOptions
}

func (o *orbitDBAccessController) Type() string {
	return "orbitdb"
}

func (o *orbitDBAccessController) Address() address.Address {
	return o.kvStore.Address()
}

func (o *orbitDBAccessController) GetAuthorizedByRole(role string) ([]string, error) {
	authorizations, err := o.getAuthorizations()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get authorizations")
	}

	return authorizations[role], nil
}

func (o *orbitDBAccessController) getAuthorizations() (map[string][]string, error) {
	authorizations := map[string]map[string]struct{}{}

	if o.kvStore == nil {
		return map[string][]string{}, nil
	}

	for role, keyBytes := range o.kvStore.All() {
		var authorizedKeys []string
		authorizations[role] = map[string]struct{}{}

		if err := json.Unmarshal(keyBytes, &authorizedKeys); err != nil {
			return nil, errors.Wrap(err, "unable to unmarshal json")
		}

		for _, key := range authorizedKeys {
			authorizations[role][key] = struct{}{}
		}
	}

	if _, ok := authorizations["write"]; ok {
		if _, ok := authorizations["admin"]; !ok {
			authorizations["admin"] = map[string]struct{}{}
		}

		for authorized := range authorizations["write"] {
			authorizations["admin"][authorized] = struct{}{}
		}
	}

	authorizationsLists := map[string][]string{}

	for permissionName, authorizationMap := range authorizations {
		authorizationsLists[permissionName] = []string{}
		for authorized := range authorizationMap {
			authorizationsLists[permissionName] = append(authorizationsLists[permissionName], authorized)
		}
	}

	return authorizationsLists, nil
}

func (o *orbitDBAccessController) CanAppend(entry *entry.Entry, p identityprovider.Interface) error {
	writeAccess, err := o.GetAuthorizedByRole("write")
	if err != nil {
		return errors.Wrap(err, "unable to get keys with write access")
	}

	adminAccess, err := o.GetAuthorizedByRole("admin")
	if err != nil {
		return errors.Wrap(err, "unable to get keys with admin access")
	}

	access := append(writeAccess, adminAccess...)

	for _, k := range access {
		if k == entry.Identity.ID || k == "*" {
			return p.VerifyIdentity(entry.Identity)
		}
	}

	return errors.New("unauthorized")
}

func (o *orbitDBAccessController) Grant(ctx context.Context, capability string, keyID string) error {
	capabilities, err := o.GetAuthorizedByRole(capability)
	if err != nil {
		return errors.Wrap(err, "unable to fetch capabilities")
	}

	capabilities = append(capabilities, keyID)

	capabilitiesJSON, err := json.Marshal(capabilities)
	if err != nil {
		return errors.Wrap(err, "unable to marshal capabilities")
	}

	_, err = o.kvStore.Put(ctx, capability, capabilitiesJSON)
	if err!= nil {
		return errors.Wrap(err, "unable to put data in store")
	}

	return nil
}

func (o *orbitDBAccessController) Revoke(ctx context.Context, capability string, keyID string) error {
	capabilities, err := o.GetAuthorizedByRole(capability)
	if err != nil {
		return errors.Wrap(err, "unable to get capability")
	}

	for idx, existingKeyID := range capabilities {
		if existingKeyID == keyID {
			capabilities = append(capabilities[:idx], capabilities[idx+1:]...)
			break
		}
	}

	if len(capabilities) > 0 {
		capabilitiesJSON, err := json.Marshal(capabilities)
		if err != nil {
			return errors.Wrap(err, "unable to marshal capabilities")
		}

		_, err = o.kvStore.Put(ctx, capability, capabilitiesJSON)
		if err != nil {
			return errors.Wrap(err, "unable to persist capabilities")
		}
	} else {
		_, err := o.kvStore.Delete(ctx, capability)
		if err != nil {
			return errors.Wrap(err, "unable to remove capabilities")
		}
	}

	return nil
}

func (o *orbitDBAccessController) Load(ctx context.Context, address string) error {
	if o.kvStore != nil {
		err := o.kvStore.Close()
		if err != nil {
			return errors.Wrap(err, "unable to close opened store")
		}
	}

	// Force '<address>/_access' naming for the database
	writeAccess := o.options.AdminAccess
	if len(writeAccess) == 0 {
		writeAccess = []string{o.orbitdb.Identity().ID}
	}

	ipfsAccessController, err := ipfs.NewIPFSAccessController(ctx, o.orbitdb, &base.CreateAccessControllerOptions{
		WriteAccess: writeAccess,
	})
	if err != nil {
		return errors.New("unable to create IPFS access controller")
	}

	store, err := o.orbitdb.KeyValue(ctx, utils.EnsureAddress(address), &orbitdb.CreateDBOptions{
		AccessController: ipfsAccessController,
	})
	if err != nil {
		return errors.Wrap(err, "unable to open key value store for access controller")
	}

	o.kvStore = store

	eventChan := o.kvStore.Subscribe()
	go func() {
		select {
		case e := <-eventChan:
			switch e.(type) {
			case stores.EventReady, stores.EventWrite, stores.EventReplicated:
				o.onUpdate()
			}
		case <-ctx.Done():
			break
		}
	}()

	err = o.kvStore.Load(ctx, -1)
	if err != nil {
		return errors.Wrap(err, "unable to fetch store data")
	}

	return nil
}

func (o *orbitDBAccessController) Save(ctx context.Context) (accesscontroller.ManifestParams, error) {
	return accesscontroller.NewManifestParams(o.kvStore.Address().GetRoot(), false, "orbitdb"), nil
}

func (o *orbitDBAccessController) Close() error {
	if err := o.kvStore.Close(); err != nil {
		return errors.Wrap(err, "error while closing store")
	}

	return nil
}

func (o *orbitDBAccessController) onUpdate() {
	o.Emit(&EventUpdated{})
}

func NewOrbitDBAccessController(ctx context.Context, db orbitdb.OrbitDB, options *base.CreateAccessControllerOptions) (accesscontroller.Interface, error) {
	if db == nil {
		return &orbitDBAccessController{}, errors.New("an OrbitDB instance is required")
	}

	addr := "default-access-controller"
	if options.Address != "" {
		addr = options.Address
	} else if options.Name != "" {
		addr = options.Name
	}

	kvStore, err := db.KeyValue(ctx, addr, nil)
	if err != nil {
		return nil, errors.Wrap(err, "unable to init key value store")
	}

	controller := &orbitDBAccessController{
		kvStore: kvStore,
		options: options,
	}

	for _, writeAccess := range options.WriteAccess {
		if err := controller.Grant(ctx, "write", writeAccess); err != nil {
			return nil, errors.Wrap(err, "unable to grant write access")
		}
	}

	return controller, nil
}

var _ accesscontroller.Interface = &orbitDBAccessController{}

func init() {
	_ = base.AddAccessController(NewOrbitDBAccessController)
}
