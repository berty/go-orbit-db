package baseorbitdb

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/io"
	"berty.tech/go-ipfs-log/keystore"
	"berty.tech/go-orbit-db/accesscontroller"
	acutils "berty.tech/go-orbit-db/accesscontroller/utils"
	"berty.tech/go-orbit-db/address"
	"berty.tech/go-orbit-db/cache"
	"berty.tech/go-orbit-db/cache/cacheleveldown"
	"berty.tech/go-orbit-db/iface"
	_ "berty.tech/go-orbit-db/internal/buildconstraints" // fail for bad go version
	"berty.tech/go-orbit-db/messagemarshaler"
	"berty.tech/go-orbit-db/pubsub"
	"berty.tech/go-orbit-db/pubsub/oneonone"
	"berty.tech/go-orbit-db/utils"
	coreapi "github.com/ipfs/boxo/coreiface"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/libp2p/go-libp2p/core/event"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// OrbitDB An alias of the type defined in the iface package
type BaseOrbitDB = iface.BaseOrbitDB

// Store An alias of the type defined in the iface package
type Store = iface.Store

// EventLogStore An alias of the type defined in the iface package
type EventLogStore = iface.EventLogStore

// KeyValueStore An alias of the type defined in the iface package
type KeyValueStore = iface.KeyValueStore

// StoreIndex An alias of the type defined in the iface package
type StoreIndex = iface.StoreIndex

// StoreConstructor An alias of the type defined in the iface package
type StoreConstructor = iface.StoreConstructor

// IndexConstructor An alias of the type defined in the iface package
type IndexConstructor = iface.IndexConstructor

// OnWritePrototype An alias of the type defined in the iface package
type OnWritePrototype = iface.OnWritePrototype

// StreamOptions An alias of the type defined in the iface package
type StreamOptions = iface.StreamOptions

// CreateDBOptions An alias of the type defined in the iface package
type CreateDBOptions = iface.CreateDBOptions

// DetermineAddressOptions An alias of the type defined in the iface package
type DetermineAddressOptions = iface.DetermineAddressOptions

// DirectChannelFactory An alias of the type defined in the iface package
type DirectChannelFactory = iface.DirectChannelFactory

func boolPtr(val bool) *bool {
	return &val
}

const CBORReadDefaultTimeout = time.Second * 30

// NewOrbitDBOptions Options for a new OrbitDB instance
type NewOrbitDBOptions struct {
	ID                   *string
	PeerID               peer.ID
	Directory            *string
	Keystore             keystore.Interface
	Cache                cache.Interface
	Identity             *idp.Identity
	CloseKeystore        func() error
	Logger               *zap.Logger
	Tracer               trace.Tracer
	DirectChannelFactory iface.DirectChannelFactory
	PubSub               iface.PubSubInterface
	MessageMarshaler     iface.MessageMarshaler
	EventBus             event.Bus
}

type orbitDB struct {
	ctx                   context.Context
	cancel                context.CancelFunc
	storeTypes            map[string]iface.StoreConstructor
	accessControllerTypes map[string]iface.AccessControllerConstructor
	ipfs                  coreapi.CoreAPI
	identity              *idp.Identity
	id                    peer.ID
	pubsub                iface.PubSubInterface
	keystore              keystore.Interface
	closeKeystore         func() error
	stores                map[string]Store
	eventBus              event.Bus
	directory             string
	cache                 cache.Interface
	logger                *zap.Logger
	tracer                trace.Tracer
	directChannel         iface.DirectChannel
	messageMarshaler      iface.MessageMarshaler

	// emitters
	emitters struct {
		newHeads event.Emitter
	}

	muStoreTypes            sync.RWMutex
	muStores                sync.RWMutex
	muIdentity              sync.RWMutex
	muID                    sync.RWMutex
	muIPFS                  sync.RWMutex
	muKeyStore              sync.RWMutex
	muCaches                sync.RWMutex
	muAccessControllerTypes sync.RWMutex
}

func (o *orbitDB) Logger() *zap.Logger {
	return o.logger
}

func (o *orbitDB) Tracer() trace.Tracer {
	return o.tracer
}

func (o *orbitDB) IPFS() coreapi.CoreAPI {
	o.muIPFS.RLock()
	defer o.muIPFS.RUnlock()

	return o.ipfs
}

func (o *orbitDB) Identity() *idp.Identity {
	o.muIdentity.RLock()
	defer o.muIdentity.RUnlock()

	return o.identity
}

func (o *orbitDB) PeerID() peer.ID {
	o.muID.RLock()
	defer o.muID.RUnlock()

	return o.id
}

func (o *orbitDB) KeyStore() keystore.Interface {
	// TODO: check why o.keystore is never set
	o.muKeyStore.RLock()
	defer o.muKeyStore.RUnlock()

	return o.keystore
}

func (o *orbitDB) CloseKeyStore() func() error {
	// TODO: check why o.closeKeystore is never set
	o.muKeyStore.RLock()
	defer o.muKeyStore.RUnlock()

	return o.closeKeystore
}

func (o *orbitDB) setStore(address string, store iface.Store) {
	o.muStores.Lock()
	defer o.muStores.Unlock()

	o.stores[address] = store
}

func (o *orbitDB) deleteStore(address string) {
	o.muStores.Lock()
	defer o.muStores.Unlock()

	delete(o.stores, address)
}

func (o *orbitDB) getStore(address string) (iface.Store, bool) {
	o.muStores.RLock()
	defer o.muStores.RUnlock()

	store, ok := o.stores[address]

	return store, ok
}

func (o *orbitDB) closeAllStores() {
	stores := []Store{}

	o.muStores.Lock()
	for _, store := range o.stores {
		stores = append(stores, store)
	}
	o.muStores.Unlock()

	for _, store := range stores {
		if err := store.Close(); err != nil {
			o.logger.Error("unable to close store", zap.Error(err))
		}
	}
}

func (o *orbitDB) closeCache() {
	o.muCaches.Lock()
	defer o.muCaches.Unlock()

	if err := o.cache.Close(); err != nil {
		o.logger.Error("unable to close cache", zap.Error(err))
	}
}

func (o *orbitDB) closeDirectConnections() {
	if err := o.directChannel.Close(); err != nil {
		o.logger.Error("unable to close connection", zap.Error(err))
	}
}

func (o *orbitDB) closeKeyStore() {
	o.muKeyStore.Lock()
	defer o.muKeyStore.Unlock()

	if o.closeKeystore != nil {
		if err := o.closeKeystore(); err != nil {
			o.logger.Error("unable to close key store", zap.Error(err))
		}
	}
}

// GetAccessControllerType Gets an access controller type
func (o *orbitDB) GetAccessControllerType(controllerType string) (iface.AccessControllerConstructor, bool) {
	o.muAccessControllerTypes.RLock()
	defer o.muAccessControllerTypes.RUnlock()

	acType, ok := o.accessControllerTypes[controllerType]

	return acType, ok
}

func (o *orbitDB) UnregisterAccessControllerType(controllerType string) {
	o.muAccessControllerTypes.Lock()
	defer o.muAccessControllerTypes.Unlock()

	delete(o.accessControllerTypes, controllerType)
}

func (o *orbitDB) RegisterAccessControllerType(constructor iface.AccessControllerConstructor) error {
	o.muAccessControllerTypes.Lock()
	defer o.muAccessControllerTypes.Unlock()

	if constructor == nil {
		return fmt.Errorf("accessController class needs to be given as an option")
	}

	controller, _ := constructor(context.Background(), nil, nil)

	controllerType := controller.Type()

	if controller.Type() == "" {
		panic("controller type cannot be empty")
	}

	o.accessControllerTypes[controllerType] = constructor

	return nil

}

// RegisterStoreType Registers a new store type which can be used by its name
func (o *orbitDB) RegisterStoreType(storeType string, constructor iface.StoreConstructor) {
	o.muStoreTypes.Lock()
	defer o.muStoreTypes.Unlock()

	o.storeTypes[storeType] = constructor
}

// UnregisterStoreType Unregisters a store type by its name
func (o *orbitDB) UnregisterStoreType(storeType string) {
	o.muStoreTypes.Lock()
	defer o.muStoreTypes.Unlock()

	delete(o.storeTypes, storeType)
}

func (o *orbitDB) storeTypesNames() []string {
	o.muStoreTypes.RLock()
	defer o.muStoreTypes.RUnlock()

	names := make([]string, len(o.storeTypes))
	i := 0

	for k := range o.storeTypes {
		names[i] = k
		i++
	}

	return names
}

func (o *orbitDB) getStoreConstructor(s string) (iface.StoreConstructor, bool) {
	o.muStoreTypes.RLock()
	defer o.muStoreTypes.RUnlock()

	constructor, ok := o.storeTypes[s]
	return constructor, ok
}

func newOrbitDB(ctx context.Context, is coreapi.CoreAPI, identity *idp.Identity, options *NewOrbitDBOptions) (BaseOrbitDB, error) {
	if is == nil {
		return nil, fmt.Errorf("ipfs is a required argument")
	}

	if identity == nil {
		return nil, fmt.Errorf("identity is a required argument")
	}

	if options == nil {
		options = &NewOrbitDBOptions{}
	}

	if options.Logger == nil {
		options.Logger = zap.NewNop()
	}

	if options.Tracer == nil {
		options.Tracer = trace.NewNoopTracerProvider().Tracer("")
	}

	if options.EventBus == nil {
		options.EventBus = eventbus.NewBus()
	}

	if options.DirectChannelFactory == nil {
		options.DirectChannelFactory = oneonone.NewChannelFactory(is)
	}
	directConnections, err := makeDirectChannel(ctx, options.EventBus, options.DirectChannelFactory, &iface.DirectChannelOptions{
		Logger: options.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create a direct connection with peer: %w", err)
	}

	k, err := is.Key().Self(ctx)
	if err != nil {
		return nil, err
	}

	if options.PeerID.Validate() == peer.ErrEmptyPeerID {
		id := k.ID()
		options.PeerID = id
	}

	if options.MessageMarshaler == nil {
		options.MessageMarshaler = &messagemarshaler.JSONMarshaler{}
	}

	if options.Cache == nil {
		options.Cache = cacheleveldown.New(&cache.Options{Logger: options.Logger})
	}

	if options.Directory == nil {
		options.Directory = &cacheleveldown.InMemoryDirectory
	}

	ctx, cancel := context.WithCancel(ctx)
	odb := &orbitDB{
		ctx:                   ctx,
		cancel:                cancel,
		ipfs:                  is,
		identity:              identity,
		id:                    options.PeerID,
		pubsub:                options.PubSub,
		cache:                 options.Cache,
		directory:             *options.Directory,
		eventBus:              options.EventBus,
		stores:                map[string]Store{},
		directChannel:         directConnections,
		closeKeystore:         options.CloseKeystore,
		storeTypes:            map[string]iface.StoreConstructor{},
		accessControllerTypes: map[string]iface.AccessControllerConstructor{},
		logger:                options.Logger,
		tracer:                options.Tracer,
		messageMarshaler:      options.MessageMarshaler,
	}

	// set new heads as stateful, so newly subscriber can replay last event in case they missed it
	odb.emitters.newHeads, err = options.EventBus.Emitter(new(EventExchangeHeads), eventbus.Stateful)
	if err != nil {
		return nil, fmt.Errorf("unable to create global emitter: %w", err)
	}

	if err := odb.monitorDirectChannel(ctx, options.EventBus); err != nil {
		return nil, fmt.Errorf("unable to monitor direct channel: %w", err)
	}

	return odb, nil
}

// NewOrbitDB Creates a new OrbitDB instance
func NewOrbitDB(ctx context.Context, ipfs coreapi.CoreAPI, options *NewOrbitDBOptions) (BaseOrbitDB, error) {
	if ipfs == nil {
		return nil, fmt.Errorf("ipfs is a required argument")
	}

	k, err := ipfs.Key().Self(ctx)
	if err != nil {
		return nil, err
	}

	id := k.ID()

	if options == nil {
		options = &NewOrbitDBOptions{}
	}

	if options.Directory == nil {
		options.Directory = &cacheleveldown.InMemoryDirectory
	}

	if options.Keystore == nil {
		var err error
		var ds *leveldb.Datastore

		// create new datastore
		if *options.Directory == cacheleveldown.InMemoryDirectory {
			ds, err = leveldb.NewDatastore("", nil)
		} else {
			ds, err = leveldb.NewDatastore(path.Join(*options.Directory, id.String(), "/keystore"), nil)
		}

		if err != nil {
			return nil, fmt.Errorf("unable to create data store used by keystore: %w", err)
		}

		ks, err := keystore.NewKeystore(ds)
		if err != nil {
			return nil, fmt.Errorf("unable to create keystore: %w", err)
		}

		options.Keystore = ks
		options.CloseKeystore = ds.Close
	}

	if options.ID == nil {
		id := id.String()
		options.ID = &id
	}

	if options.Identity == nil {
		identity, err := idp.CreateIdentity(ctx, &idp.CreateIdentityOptions{
			Keystore: options.Keystore,
			Type:     "orbitdb",
			ID:       *options.ID,
		})

		if err != nil {
			return nil, err
		}

		options.Identity = identity
	}

	return newOrbitDB(ctx, ipfs, options.Identity, options)
}

func (o *orbitDB) Close() error {
	o.closeAllStores()
	o.closeDirectConnections()
	o.closeCache()
	o.closeKeyStore()

	if err := o.emitters.newHeads.Close(); err != nil {
		o.logger.Warn("unable to close emitter", zap.Error(err))
	}

	o.cancel()
	return nil
}

func (o *orbitDB) Create(ctx context.Context, name string, storeType string, options *CreateDBOptions) (Store, error) {
	o.logger.Debug("Create()")

	if options == nil {
		options = &CreateDBOptions{}
	}

	// The directory to look databases from can be passed in as an option
	if options.Directory == nil {
		options.Directory = &o.directory
	}

	o.logger.Debug(fmt.Sprintf("Creating database '%s' as %s in '%s'", name, storeType, o.directory))

	// Create the database address
	dbAddress, err := o.DetermineAddress(ctx, name, storeType, &DetermineAddressOptions{AccessController: options.AccessController})
	if err != nil {
		return nil, err
	}

	// Load the locally saved database information
	c, err := o.loadCache(o.directory, dbAddress)
	if err != nil {
		return nil, fmt.Errorf("unable to load cache: %w", err)
	}

	// Check if we have the database locally
	haveDB := o.haveLocalData(ctx, c, dbAddress)

	if haveDB && (options.Overwrite == nil || !*options.Overwrite) {
		return nil, fmt.Errorf("database %s already exists", dbAddress)
	}

	// Save the database locally
	if err := o.addManifestToCache(ctx, o.directory, dbAddress); err != nil {
		return nil, fmt.Errorf("unable to add manifest to cache: %w", err)
	}

	o.logger.Debug(fmt.Sprintf("Created database '%s'", dbAddress))

	// Open the database
	return o.Open(ctx, dbAddress.String(), options)
}

// Open opens a OrbitDB store. During this operation, ctx can be used to cancel or expire this method. Once this function returns, the cancellation and expiration of ctx will be noop. Users should call Store.Close to close the store after this function returns.
func (o *orbitDB) Open(ctx context.Context, dbAddress string, options *CreateDBOptions) (Store, error) {
	o.logger.Debug("open orbitdb store", zap.String("address", dbAddress))

	if options == nil {
		options = &CreateDBOptions{}
	}

	if options.Timeout == 0 {
		options.Timeout = CBORReadDefaultTimeout
	}

	if options.LocalOnly == nil {
		options.LocalOnly = boolPtr(false)
	}

	if options.Create == nil {
		options.Create = boolPtr(false)
	}

	if options.IO == nil {
		options.IO = io.CBOR()
	}

	o.logger.Debug("Open database ", zap.String("dbAddress", dbAddress))

	directory := o.directory
	if options.Directory != nil {
		directory = *options.Directory
	}

	o.logger.Debug("Look from ", zap.String("directory", directory))

	if err := address.IsValid(dbAddress); err != nil {
		o.logger.Warn("open: invalid OrbitDB address", zap.String("address", dbAddress))
		if !*options.Create {
			return nil, fmt.Errorf("'options.Create' set to 'false'. If you want to create a database, set 'options.Create' to 'true'")
		} else if *options.Create && (options.StoreType == nil || *options.StoreType == "") {
			return nil, fmt.Errorf("database type not provided! Provide a type with 'options.StoreType' (%s)", strings.Join(o.storeTypesNames(), "|"))
		} else {
			options.Overwrite = boolPtr(true)
			return o.Create(ctx, dbAddress, *options.StoreType, options)
		}
	}

	parsedDBAddress, err := address.Parse(dbAddress)
	if err != nil {
		return nil, fmt.Errorf("unable to parse address: %w", err)
	}

	dbCache, err := o.loadCache(directory, parsedDBAddress)
	if err != nil {
		return nil, fmt.Errorf("unable to acquire cache: %w", err)
	}

	haveDB := o.haveLocalData(ctx, dbCache, parsedDBAddress)
	if *options.LocalOnly && !haveDB {
		return nil, fmt.Errorf("database doesn't exist: %s", dbAddress)
	}

	readctx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()

	manifestNode, err := io.ReadCBOR(readctx, o.IPFS(), parsedDBAddress.GetRoot())
	if err != nil {
		return nil, fmt.Errorf("unable to fetch database manifest: %w", err)
	}

	manifest := &utils.Manifest{}
	if err := cbornode.DecodeInto(manifestNode.RawData(), manifest); err != nil {
		return nil, fmt.Errorf("unable to unmarshal manifest: %w", err)
	}

	o.logger.Debug("Creating store instance")

	options.AccessControllerAddress = manifest.AccessController

	store, err := o.createStore(ctx, manifest.Type, parsedDBAddress, options)
	if err != nil {
		return nil, fmt.Errorf("unable to create store: %w", err)
	}

	return store, nil
}

func (o *orbitDB) DetermineAddress(ctx context.Context, name string, storeType string, options *DetermineAddressOptions) (address.Address, error) {
	var err error

	if options == nil {
		options = &DetermineAddressOptions{}
	}

	if options.OnlyHash == nil {
		t := true
		options.OnlyHash = &t
	}

	if _, ok := o.getStoreConstructor(storeType); !ok {
		return nil, fmt.Errorf("invalid database type")
	}

	if err := address.IsValid(name); err == nil {
		return nil, fmt.Errorf("given database name is an address, give only the name of the database")
	}

	if options.AccessController == nil {
		options.AccessController = accesscontroller.NewEmptyManifestParams()
	}

	if options.AccessController.GetName() == "" {
		options.AccessController.SetName(name)
	}

	if options.AccessController.GetType() == "" {
		options.AccessController.SetType("ipfs")
	}

	accessControllerAddress, err := acutils.Create(ctx, o, options.AccessController.GetType(), options.AccessController, accesscontroller.WithLogger(o.logger))
	if err != nil {
		return nil, fmt.Errorf("unable to create access controller: %w", err)
	}

	// Save the manifest to IPFS
	manifestHash, err := utils.CreateDBManifest(ctx, o.IPFS(), name, storeType, accessControllerAddress.String())
	if err != nil {
		return nil, fmt.Errorf("unable to save manifest on ipfs: %w", err)
	}

	// Create the database address
	return address.Parse(path.Join("/orbitdb", manifestHash.String(), name))
}

func (o *orbitDB) loadCache(directory string, dbAddress address.Address) (datastore.Datastore, error) {
	db, err := o.cache.Load(directory, dbAddress)
	if err != nil {
		return nil, fmt.Errorf("unable to load cache: %w", err)
	}

	return db, nil
}

func (o *orbitDB) haveLocalData(ctx context.Context, c datastore.Datastore, dbAddress address.Address) bool {
	if c == nil {
		o.logger.Debug("haveLocalData: no cache provided")
		return false
	}

	cacheKey := datastore.NewKey(path.Join(dbAddress.String(), "_manifest"))

	data, err := c.Get(ctx, cacheKey)
	if err != nil {
		if err != datastore.ErrNotFound {
			o.logger.Error("haveLocalData: error while getting value from cache", zap.Error(err))
		}

		return false
	}

	return data != nil
}

func (o *orbitDB) addManifestToCache(ctx context.Context, directory string, dbAddress address.Address) error {
	c, err := o.loadCache(directory, dbAddress)
	if err != nil {
		return fmt.Errorf("unable to load existing cache: %w", err)
	}

	cacheKey := datastore.NewKey(path.Join(dbAddress.String(), "_manifest"))

	if err := c.Put(ctx, cacheKey, []byte(dbAddress.GetRoot().String())); err != nil {
		return fmt.Errorf("unable to set cache: %w", err)
	}

	return nil
}

func (o *orbitDB) createStore(ctx context.Context, storeType string, parsedDBAddress address.Address, options *CreateDBOptions) (Store, error) {
	var err error
	storeFunc, ok := o.getStoreConstructor(storeType)
	if !ok {
		return nil, fmt.Errorf("store type %s is not supported", storeType)
	}

	var accessController accesscontroller.Interface
	options.AccessControllerAddress = strings.TrimPrefix(options.AccessControllerAddress, "/ipfs/")

	if options.AccessControllerAddress != "" {
		o.logger.Debug(fmt.Sprintf("Access controller address is %s", options.AccessControllerAddress))

		c, _ := cid.Decode(options.AccessControllerAddress)

		ac := options.AccessController
		if ac == nil {
			ac = accesscontroller.NewEmptyManifestParams()
		} else {
			ac = accesscontroller.CloneManifestParams(options.AccessController)
		}
		ac.SetAddress(c)

		accessController, err = acutils.Resolve(ctx, o, options.AccessControllerAddress, ac, accesscontroller.WithLogger(o.logger))
		if err != nil {
			return nil, fmt.Errorf("unable to acquire an access controller: %w", err)
		}
	}

	o.logger.Debug(fmt.Sprintf("loading cache for db %s", parsedDBAddress.String()))

	c, err := o.loadCache(o.directory, parsedDBAddress)
	if err != nil {
		return nil, fmt.Errorf("unable to acquire a cache instance: %w", err)
	}

	if options.Replicate == nil {
		options.Replicate = boolPtr(true)
	}

	// options.AccessController = accessController
	options.Keystore = o.KeyStore()
	options.Cache = c

	identity := o.Identity()
	if options.Identity != nil {
		identity = options.Identity
	}

	if options.Directory == nil {
		options.Directory = &o.directory
	}

	if options.EventBus == nil {
		if o.EventBus() != nil {
			options.EventBus = o.EventBus()
		} else {
			options.EventBus = eventbus.NewBus()
		}
	}

	if options.Logger == nil {
		options.Logger = o.logger
	}

	if options.CloseFunc == nil {
		options.CloseFunc = func() {}
	}
	closeFunc := func() {
		options.CloseFunc()
		o.deleteStore(parsedDBAddress.String())
	}

	store, err := storeFunc(o.IPFS(), identity, parsedDBAddress, &iface.NewStoreOptions{
		EventBus:          options.EventBus,
		AccessController:  accessController,
		Cache:             options.Cache,
		Replicate:         options.Replicate,
		Directory:         *options.Directory,
		SortFn:            options.SortFn,
		CacheDestroy:      func() error { return o.cache.Destroy(o.directory, parsedDBAddress) },
		Logger:            options.Logger,
		Tracer:            o.tracer,
		IO:                options.IO,
		StoreSpecificOpts: options.StoreSpecificOpts,
		PubSub:            o.pubsub,
		MessageMarshaler:  o.messageMarshaler,
		PeerID:            o.id,
		DirectChannel:     o.directChannel,
		CloseFunc:         closeFunc,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to instantiate store: %w", err)
	}

	o.setStore(parsedDBAddress.String(), store)

	return store, nil
}

func (o *orbitDB) EventBus() event.Bus {
	return o.eventBus
}

func (o *orbitDB) monitorDirectChannel(ctx context.Context, bus event.Bus) error {
	sub, err := bus.Subscribe(new(iface.EventPubSubPayload),
		eventbus.BufSize(128), eventbus.Name("odb/monitor-direct-channel"))
	if err != nil {
		return fmt.Errorf("unable to init pubsub subscriber: %w", err)
	}

	go func() {
		for {
			var e interface{}
			select {
			case <-ctx.Done():
				return
			case e = <-sub.Out():
			}

			evt := e.(iface.EventPubSubPayload)

			msg := iface.MessageExchangeHeads{}
			if err := o.messageMarshaler.Unmarshal(evt.Payload, &msg); err != nil {
				o.logger.Error("unable to unmarshal message payload", zap.Error(err))
				continue
			}

			store, ok := o.getStore(msg.Address)
			if !ok {
				o.logger.Error("unable to get store from address", zap.Error(err))
				continue
			}

			if err := o.handleEventExchangeHeads(ctx, &msg, store); err != nil {
				o.logger.Error("unable to handle pubsub payload", zap.Error(err))
				continue
			}

			if err := o.emitters.newHeads.Emit(NewEventExchangeHeads(evt.Peer, &msg)); err != nil {
				o.logger.Warn("unable to emit new heads", zap.Error(err))
			}
		}
	}()

	return nil
}

func makeDirectChannel(ctx context.Context, bus event.Bus, df iface.DirectChannelFactory, opts *iface.DirectChannelOptions) (iface.DirectChannel, error) {
	emitter, err := pubsub.NewPayloadEmitter(bus)
	if err != nil {
		return nil, fmt.Errorf("unable to init pubsub emitter: %w", err)
	}

	return df(ctx, emitter, opts)
}

var _ BaseOrbitDB = &orbitDB{}
