package orbitdb

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	logac "github.com/stateless-minds/go-ipfs-log/accesscontroller"
	"github.com/stateless-minds/go-ipfs-log/identityprovider"
	"github.com/stateless-minds/go-orbit-db/accesscontroller"
	"github.com/stateless-minds/go-orbit-db/accesscontroller/utils"
	"github.com/stateless-minds/go-orbit-db/address"
	"github.com/stateless-minds/go-orbit-db/iface"
	"github.com/stateless-minds/go-orbit-db/stores"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
	"go.uber.org/zap"
)

var Events = []interface{}{
	new(EventUpdated),
}

// CreateDBOptions An alias for iface.CreateDBOptions
type CreateDBOptions = iface.CreateDBOptions

// EventUpdated An event sent when the access controller has been updated
type EventUpdated struct{}

type orbitDBAccessController struct {
	emitterEvtUpdated event.Emitter
	orbitdb           iface.OrbitDB
	kvStore           iface.KeyValueStore
	options           accesscontroller.ManifestParams
	lock              sync.RWMutex
	logger            *zap.Logger
}

func (o *orbitDBAccessController) SetLogger(logger *zap.Logger) {
	o.lock.Lock()
	defer o.lock.Unlock()

	o.logger = logger
}

func (o *orbitDBAccessController) Logger() *zap.Logger {
	o.lock.RLock()
	defer o.lock.RUnlock()

	return o.logger
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
		return nil, fmt.Errorf("unable to get authorizations: %w", err)
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
			return nil, fmt.Errorf("unable to unmarshal json: %w", err)
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

func (o *orbitDBAccessController) CanAppend(entry logac.LogEntry, p identityprovider.Interface, additionalContext accesscontroller.CanAppendAdditionalContext) error {
	writeAccess, err := o.GetAuthorizedByRole("write")
	if err != nil {
		return fmt.Errorf("unable to get keys with write access: %w", err)
	}

	adminAccess, err := o.GetAuthorizedByRole("admin")
	if err != nil {
		return fmt.Errorf("unable to get keys with admin access: %w", err)
	}

	access := append(writeAccess, adminAccess...)

	for _, k := range access {
		if k == entry.GetIdentity().ID || k == "*" {
			return p.VerifyIdentity(entry.GetIdentity())
		}
	}

	return fmt.Errorf("unauthorized")
}

func (o *orbitDBAccessController) Grant(ctx context.Context, capability string, keyID string) error {
	capabilities, err := o.GetAuthorizedByRole(capability)
	if err != nil {
		return fmt.Errorf("unable to fetch capabilities: %w", err)
	}

	capabilities = append(capabilities, keyID)

	capabilitiesJSON, err := json.Marshal(capabilities)
	if err != nil {
		return fmt.Errorf("unable to marshal capabilities: %w", err)
	}

	_, err = o.kvStore.Put(ctx, capability, capabilitiesJSON)
	if err != nil {
		return fmt.Errorf("unable to put data in store: %w", err)
	}

	return nil
}

func (o *orbitDBAccessController) Revoke(ctx context.Context, capability string, keyID string) error {
	capabilities, err := o.GetAuthorizedByRole(capability)
	if err != nil {
		return fmt.Errorf("unable to get capability: %w", err)
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
			return fmt.Errorf("unable to marshal capabilities: %w", err)
		}

		_, err = o.kvStore.Put(ctx, capability, capabilitiesJSON)
		if err != nil {
			return fmt.Errorf("unable to persist capabilities: %w", err)
		}
	} else {
		_, err := o.kvStore.Delete(ctx, capability)
		if err != nil {
			return fmt.Errorf("unable to remove capabilities: %w", err)
		}
	}

	return nil
}

func (o *orbitDBAccessController) Load(ctx context.Context, address string) error {
	if o.kvStore != nil {
		err := o.kvStore.Close()
		if err != nil {
			return fmt.Errorf("unable to close opened store: %w", err)
		}
	}

	// Force '<address>/_access' naming for the database
	writeAccess := o.options.GetAccess("admin")
	if len(writeAccess) == 0 {
		writeAccess = []string{o.orbitdb.Identity().ID}
	}

	ipfsAccessController := accesscontroller.NewManifestParams(cid.Cid{}, true, "ipfs")
	ipfsAccessController.SetAccess("write", writeAccess)

	store, err := o.orbitdb.KeyValue(ctx, utils.EnsureAddress(address), &CreateDBOptions{
		AccessController: ipfsAccessController,
	})
	if err != nil {
		return fmt.Errorf("unable to open key value store for access controller: %w", err)
	}

	o.kvStore = store

	sub, err := o.kvStore.EventBus().Subscribe([]interface{}{
		new(stores.EventWrite),
		new(stores.EventReady),
		new(stores.EventReplicated),
	}, eventbus.Name("odb/load"))
	if err != nil {
		return fmt.Errorf("unable subscribe to store events: %w", err)
	}

	go func() {
		defer sub.Close()

		var evt interface{}
		for {
			select {
			case <-ctx.Done():
				return
			case evt = <-sub.Out():
			}

			switch evt.(type) {
			case stores.EventReady, stores.EventWrite, stores.EventReplicated:
				o.onUpdate(ctx)
			}
		}
	}()

	err = o.kvStore.Load(ctx, -1)
	if err != nil {
		return fmt.Errorf("unable to fetch store data: %w", err)
	}

	return nil
}

func (o *orbitDBAccessController) Save(ctx context.Context) (accesscontroller.ManifestParams, error) {
	return accesscontroller.NewManifestParams(o.kvStore.Address().GetRoot(), false, "orbitdb"), nil
}

func (o *orbitDBAccessController) Close() error {
	if err := o.kvStore.Close(); err != nil {
		return fmt.Errorf("error while closing store: %w", err)
	}

	return nil
}

func (o *orbitDBAccessController) onUpdate(ctx context.Context) {
	if err := o.emitterEvtUpdated.Emit(&EventUpdated{}); err != nil {
		o.logger.Warn("unable to emit event updated", zap.Error(err))
	}
}

// NewIPFSAccessController Returns a default access controller for OrbitDB database
func NewOrbitDBAccessController(ctx context.Context, db iface.BaseOrbitDB, params accesscontroller.ManifestParams, options ...accesscontroller.Option) (accesscontroller.Interface, error) {
	if db == nil {
		return &orbitDBAccessController{}, fmt.Errorf("an OrbitDB instance is required")
	}

	kvDB, ok := db.(iface.OrbitDBKVStoreProvider)
	if !ok {
		return &orbitDBAccessController{}, fmt.Errorf("the OrbitDB instance must provide a key value store")
	}

	addr := "default-access-controller"
	if params.GetAddress().Defined() {
		addr = params.GetAddress().String()
	} else if params.GetName() != "" {
		addr = params.GetName()
	}

	kvStore, err := kvDB.KeyValue(ctx, addr, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to init key value store: %w", err)
	}

	emitter, err := db.EventBus().Emitter(new(EventUpdated))
	if err != nil {
		return nil, fmt.Errorf("unable to init emitter: %w", err)
	}

	controller := &orbitDBAccessController{
		emitterEvtUpdated: emitter,
		kvStore:           kvStore,
		options:           params,
	}

	for _, o := range options {
		o(controller)
	}

	for _, writeAccess := range params.GetAccess("write") {
		if err := controller.Grant(ctx, "write", writeAccess); err != nil {
			return nil, fmt.Errorf("unable to grant write access: %w", err)
		}
	}

	return controller, nil
}

var _ accesscontroller.Interface = &orbitDBAccessController{}
