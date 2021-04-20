package baseorbitdb

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/enc"
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/identityprovider"
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
	"berty.tech/go-orbit-db/pubsub/oneonone"
	"berty.tech/go-orbit-db/pubsub/pubsubcoreapi"
	"berty.tech/go-orbit-db/stores"
	"berty.tech/go-orbit-db/utils"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	cbornode "github.com/ipfs/go-ipld-cbor"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
	p2pcore "github.com/libp2p/go-libp2p-core"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/api/trace"
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

type exchangedHeads struct {
	Address string         `json:"address,omitempty"`
	Heads   []*entry.Entry `json:"heads,omitempty"`
}

func boolPtr(val bool) *bool {
	return &val
}

// NewOrbitDBOptions Options for a new OrbitDB instance
type NewOrbitDBOptions struct {
	ID                   *string
	PeerID               *p2pcore.PeerID
	Directory            *string
	Keystore             keystore.Interface
	Cache                cache.Interface
	Identity             *idp.Identity
	CloseKeystore        func() error
	Logger               *zap.Logger
	Tracer               trace.Tracer
	DirectChannelFactory iface.DirectChannelFactory
	PubSub               iface.PubSubInterface
}

type orbitDB struct {
	storeTypes            map[string]iface.StoreConstructor
	accessControllerTypes map[string]iface.AccessControllerConstructor
	ipfs                  coreapi.CoreAPI
	identity              *idp.Identity
	id                    p2pcore.PeerID
	pubsub                iface.PubSubInterface
	keystore              keystore.Interface
	closeKeystore         func() error
	stores                map[string]Store
	directConnections     map[p2pcore.PeerID]iface.DirectChannel
	directConnFactory     iface.DirectChannelFactory
	directory             string
	cache                 cache.Interface
	logger                *zap.Logger
	tracer                trace.Tracer

	muStoreTypes            sync.RWMutex
	muStores                sync.RWMutex
	muIdentity              sync.RWMutex
	muID                    sync.RWMutex
	muIPFS                  sync.RWMutex
	muKeyStore              sync.RWMutex
	muCaches                sync.RWMutex
	muDirectConnections     sync.RWMutex
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

func (o *orbitDB) Identity() *identityprovider.Identity {
	o.muIdentity.RLock()
	defer o.muIdentity.RUnlock()

	return o.identity
}

func (o *orbitDB) PeerID() p2pcore.PeerID {
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

func (o *orbitDB) getStore(address string) (iface.Store, bool) {
	o.muStores.RLock()
	defer o.muStores.RUnlock()

	store, ok := o.stores[address]

	return store, ok
}

func (o *orbitDB) deleteStore(address string) {
	o.muStores.Lock()
	defer o.muStores.Unlock()

	delete(o.stores, address)
}

func (o *orbitDB) closeAllStores() {
	o.muStores.Lock()
	defer o.muStores.Unlock()

	for _, store := range o.stores {
		if err := store.Close(); err != nil {
			o.logger.Error("unable to close store", zap.Error(err))
		}
	}

	o.stores = map[string]Store{}
}

func (o *orbitDB) closeCache() {
	o.muCaches.Lock()
	defer o.muCaches.Unlock()

	if err := o.cache.Close(); err != nil {
		o.logger.Error("unable to close cache", zap.Error(err))
	}
}

func (o *orbitDB) closeDirectConnections() {
	o.muDirectConnections.Lock()
	defer o.muDirectConnections.Unlock()

	for _, conn := range o.directConnections {
		if err := conn.Close(); err != nil {
			o.logger.Error("unable to close connection", zap.Error(err))
		}
	}

	o.directConnections = map[p2pcore.PeerID]iface.DirectChannel{}
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
		return errors.New("accessController class needs to be given as an option")
	}

	controller, _ := constructor(context.Background(), nil, nil)

	controllerType := controller.Type()

	if controller.Type() == "" {
		panic("controller type cannot be empty")
	}

	o.accessControllerTypes[controllerType] = constructor

	return nil

}

func (o *orbitDB) getDirectConnection(ctx context.Context, peerID p2pcore.PeerID, sharedKey enc.SharedKey) (iface.DirectChannel, error) {
	o.muDirectConnections.Lock()
	defer o.muDirectConnections.Unlock()

	if conn, ok := o.directConnections[peerID]; ok {
		return conn, nil
	}

	channel, err := o.directConnFactory(ctx, peerID, &iface.DirectChannelOptions{
		Logger: o.logger,
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to create a direct connection with peer")
	}

	o.directConnections[peerID] = channel
	o.watchOneOnOneMessage(ctx, channel, sharedKey)

	return channel, nil
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
		return nil, errors.New("ipfs is a required argument")
	}

	if identity == nil {
		return nil, errors.New("identity is a required argument")
	}

	if options == nil {
		options = &NewOrbitDBOptions{}
	}

	if options.Logger == nil {
		options.Logger = zap.NewNop()
	}

	if options.Tracer == nil {
		options.Tracer = trace.NoopTracer{}
	}

	if options.DirectChannelFactory == nil {
		options.DirectChannelFactory = oneonone.NewChannelFactory(is)
	}

	k, err := is.Key().Self(ctx)
	if err != nil {
		return nil, err
	}

	if options.PeerID == nil {
		id := k.ID()
		options.PeerID = &id
	}

	if options.PubSub == nil {
		options.PubSub = pubsubcoreapi.NewPubSub(is, k.ID(), time.Second, options.Logger, options.Tracer)
	}

	if options.Cache == nil {
		options.Cache = cacheleveldown.New(&cache.Options{Logger: options.Logger})
	}

	if options.Directory == nil {
		options.Directory = &cacheleveldown.InMemoryDirectory
	}

	return &orbitDB{
		ipfs:                  is,
		identity:              identity,
		id:                    *options.PeerID,
		pubsub:                options.PubSub,
		cache:                 options.Cache,
		directory:             *options.Directory,
		stores:                map[string]Store{},
		directConnections:     map[p2pcore.PeerID]iface.DirectChannel{},
		closeKeystore:         options.CloseKeystore,
		storeTypes:            map[string]iface.StoreConstructor{},
		accessControllerTypes: map[string]iface.AccessControllerConstructor{},
		logger:                options.Logger,
		tracer:                options.Tracer,
		directConnFactory:     options.DirectChannelFactory,
	}, nil
}

// NewOrbitDB Creates a new OrbitDB instance
func NewOrbitDB(ctx context.Context, ipfs coreapi.CoreAPI, options *NewOrbitDBOptions) (BaseOrbitDB, error) {
	if ipfs == nil {
		return nil, errors.New("ipfs is a required argument")
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
			return nil, errors.Wrap(err, "unable to create data store used by keystore")
		}

		ks, err := keystore.NewKeystore(ds)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create keystore")
		}

		options.Keystore = ks
		options.CloseKeystore = ds.Close
	}

	if options.ID == nil {
		id := id.String()
		options.ID = &id
	}

	if options.Identity == nil {
		identity, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
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
		return nil, errors.Wrap(err, "unable to load cache")
	}

	// Check if we have the database locally
	haveDB := o.haveLocalData(c, dbAddress)

	if haveDB && (options.Overwrite == nil || !*options.Overwrite) {
		return nil, errors.New(fmt.Sprintf("database %s already exists", dbAddress))
	}

	// Save the database locally
	if err := o.addManifestToCache(o.directory, dbAddress); err != nil {
		return nil, errors.Wrap(err, "unable to add manifest to cache")
	}

	o.logger.Debug(fmt.Sprintf("Created database '%s'", dbAddress))

	// Open the database
	return o.Open(ctx, dbAddress.String(), options)
}

func (o *orbitDB) Open(ctx context.Context, dbAddress string, options *CreateDBOptions) (Store, error) {
	o.logger.Debug("Open()")

	if options == nil {
		options = &CreateDBOptions{}
	}

	if options.LocalOnly == nil {
		options.LocalOnly = boolPtr(false)
	}

	if options.Create == nil {
		options.Create = boolPtr(false)
	}

	if options.IO == nil {
		cborIO := io.CBOR()
		if options.SharedKey != nil {
			cborIO.ApplyOptions(&io.CBOROptions{LinkKey: options.SharedKey})
		}

		options.IO = cborIO
	}

	o.logger.Debug("Open database ", zap.String("dbAddress", dbAddress))

	directory := o.directory
	if options.Directory != nil {
		directory = *options.Directory
	}

	o.logger.Debug("Look from ", zap.String("directory", directory))

	if err := address.IsValid(dbAddress); err != nil {
		if !*options.Create {
			return nil, errors.New("'options.Create' set to 'false'. If you want to create a database, set 'options.Create' to 'true'")
		} else if *options.Create && (options.StoreType == nil || *options.StoreType == "") {
			return nil, errors.New(fmt.Sprintf("database type not provided! Provide a type with 'options.StoreType' (%s)", strings.Join(o.storeTypesNames(), "|")))
		} else {
			o.logger.Debug(fmt.Sprintf("Not a valid OrbitDB address '%s', creating the database", dbAddress))

			options.Overwrite = boolPtr(true)

			return o.Create(ctx, dbAddress, *options.StoreType, options)
		}
	}
	o.logger.Debug(fmt.Sprintf("address '%s' is valid", dbAddress))

	parsedDBAddress, err := address.Parse(dbAddress)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse address")
	}

	dbCache, err := o.loadCache(directory, parsedDBAddress)
	if err != nil {
		return nil, errors.Wrap(err, "unable to acquire cache")
	}

	haveDB := o.haveLocalData(dbCache, parsedDBAddress)
	if *options.LocalOnly && !haveDB {
		return nil, errors.New(fmt.Sprintf("database %s doesn't exist!", dbAddress))
	}

	manifestNode, err := io.ReadCBOR(ctx, o.IPFS(), parsedDBAddress.GetRoot())
	if err != nil {
		return nil, errors.Wrap(err, "unable to fetch database manifest")
	}

	manifest := &utils.Manifest{}
	if err := cbornode.DecodeInto(manifestNode.RawData(), manifest); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal manifest")
	}

	o.logger.Debug("Creating store instance")

	options.AccessControllerAddress = manifest.AccessController

	store, err := o.createStore(ctx, manifest.Type, parsedDBAddress, options)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create store")
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
		return nil, errors.New("invalid database type")
	}

	if err := address.IsValid(name); err == nil {
		return nil, errors.New("given database name is an address, give only the name of the database")
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
		return nil, errors.Wrap(err, "unable to create access controller")
	}

	// Save the manifest to IPFS
	manifestHash, err := utils.CreateDBManifest(ctx, o.IPFS(), name, storeType, accessControllerAddress.String())
	if err != nil {
		return nil, errors.Wrap(err, "unable to save manifest on ipfs")
	}

	// Create the database address
	return address.Parse(path.Join("/orbitdb", manifestHash.String(), name))
}

func (o *orbitDB) loadCache(directory string, dbAddress address.Address) (datastore.Datastore, error) {
	db, err := o.cache.Load(directory, dbAddress)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load cache")
	}

	return db, nil
}

func (o *orbitDB) haveLocalData(c datastore.Datastore, dbAddress address.Address) bool {
	if c == nil {
		o.logger.Debug("haveLocalData: no cache provided")
		return false
	}

	cacheKey := datastore.NewKey(path.Join(dbAddress.String(), "_manifest"))

	data, err := c.Get(cacheKey)
	if err != nil {
		if err != datastore.ErrNotFound {
			o.logger.Error("haveLocalData: error while getting value from cache", zap.Error(err))
		}

		return false
	}

	return data != nil
}

func (o *orbitDB) addManifestToCache(directory string, dbAddress address.Address) error {
	c, err := o.loadCache(directory, dbAddress)
	if err != nil {
		return errors.Wrap(err, "unable to load existing cache")
	}

	cacheKey := datastore.NewKey(path.Join(dbAddress.String(), "_manifest"))

	if err := c.Put(cacheKey, []byte(dbAddress.GetRoot().String())); err != nil {
		return errors.Wrap(err, "unable to set cache")
	}

	return nil
}

func (o *orbitDB) createStore(ctx context.Context, storeType string, parsedDBAddress address.Address, options *CreateDBOptions) (Store, error) {
	var err error
	storeFunc, ok := o.getStoreConstructor(storeType)
	if !ok {
		return nil, errors.New(fmt.Sprintf("store type %s is not supported", storeType))
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
			return nil, errors.Wrap(err, "unable to acquire an access controller")
		}
	}

	o.logger.Debug(fmt.Sprintf("loading cache for db %s", parsedDBAddress.String()))

	c, err := o.loadCache(o.directory, parsedDBAddress)
	if err != nil {
		return nil, errors.Wrap(err, "unable to acquire a cache instance")
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

	store, err := storeFunc(ctx, o.IPFS(), identity, parsedDBAddress, &iface.NewStoreOptions{
		AccessController: accessController,
		Cache:            options.Cache,
		Replicate:        options.Replicate,
		Directory:        *options.Directory,
		SortFn:           options.SortFn,
		CacheDestroy:     func() error { return o.cache.Destroy(o.directory, parsedDBAddress) },
		Logger:           o.logger,
		Tracer:           o.tracer,
		IO:               options.IO,
		SharedKey:        options.SharedKey,
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate store")
	}

	topic, err := o.pubsub.TopicSubscribe(ctx, parsedDBAddress.String())
	if err != nil {
		return nil, errors.Wrap(err, "unable to subscribe to pubsub")
	}

	o.storeListener(ctx, store, topic)
	o.setStore(parsedDBAddress.String(), store)

	// Subscribe to pubsub to get updates from peers,
	// this is what hooks us into the message propagation layer
	// and the p2p network
	if *options.Replicate {
		if err := o.pubSubChanListener(ctx, store, topic, parsedDBAddress); err != nil {
			return nil, err
		}
	}

	return store, nil
}

func (o *orbitDB) onClose(addr cid.Cid) error {
	o.deleteStore(addr.String())

	return nil
}

func (o *orbitDB) storeListener(ctx context.Context, store Store, topic iface.PubSubTopic) {
	sub := store.Subscribe(ctx)
	go func() {
		for evt := range sub {
			switch e := evt.(type) {
			case *stores.EventWrite:
				o.logger.Debug("received stores.write event")
				if len(e.Heads) == 0 {
					o.logger.Debug("'heads' are not defined")
					continue
				}

				if topic != nil {
					headsBytes, err := json.Marshal(e.Heads)
					if err != nil {
						o.logger.Debug(fmt.Sprintf("unable to serialize heads %v", err))
						continue
					}

					if key := store.SharedKey(); key != nil {
						headsBytes, err = key.Seal(headsBytes)
						if err != nil {
							o.logger.Error(fmt.Sprintf("unable to encrypt heads %v", err))
							continue
						}
					}

					err = topic.Publish(ctx, headsBytes)
					if err != nil {
						o.logger.Debug(fmt.Sprintf("unable to publish message on pubsub %v", err))
						continue
					}

					o.logger.Debug("stores.write event: published event on pub sub")
				}
			}
		}

		o.logger.Debug("received stores.close event")

		if err := o.onClose(store.Address().GetRoot()); err != nil {
			o.logger.Debug(fmt.Sprintf("unable to perform onClose %v", err))
		}
	}()
}

func (o *orbitDB) pubSubChanListener(ctx context.Context, store Store, topic iface.PubSubTopic, addr address.Address) error {
	chPeers, err := topic.WatchPeers(ctx)
	if err != nil {
		return err
	}

	chMessages, err := topic.WatchMessages(ctx)
	if err != nil {
		return err
	}

	go func() {
		for e := range chPeers {
			switch evt := e.(type) {
			case *iface.EventPubSubJoin:
				go func() {
					o.onNewPeerJoined(ctx, evt.Peer, store)
					o.logger.Debug(fmt.Sprintf("peer %s joined from %s self is %s", evt.Peer.String(), addr, o.PeerID()))
				}()

			case *iface.EventPubSubLeave:
				o.logger.Debug(fmt.Sprintf("peer %s left from %s self is %s", evt.Peer.String(), addr, o.PeerID()))

			default:
				o.logger.Debug("unhandled event, can't match type")
			}
		}
	}()

	go func() {
		for evt := range chMessages {
			o.logger.Debug("Got pub sub message")

			headsEntriesBytes := evt.Content
			var headsEntries []*entry.Entry

			if key := store.SharedKey(); key != nil {
				headsEntriesBytes, err = key.Open(headsEntriesBytes)
				if err != nil {
					o.logger.Error("unable to decrypt head entries", zap.Error(err))
					continue
				}
			}

			err := json.Unmarshal(headsEntriesBytes, &headsEntries)
			if err != nil {
				o.logger.Error("unable to unmarshal head entries", zap.Error(err))
				continue
			}

			if len(headsEntries) == 0 {
				o.logger.Debug(fmt.Sprintf("Nothing to synchronize for %s:", addr))
				continue
			}

			o.logger.Debug(fmt.Sprintf("Received %d heads for %s:", len(headsEntries), addr))

			entries := make([]ipfslog.Entry, len(headsEntries))
			for i := range headsEntries {
				entries[i] = headsEntries[i]
			}

			if err := store.Sync(ctx, entries); err != nil {
				o.logger.Debug(fmt.Sprintf("Error while syncing heads for %s:", addr))
			}
		}
	}()

	return nil
}

func (o *orbitDB) onNewPeerJoined(ctx context.Context, p p2pcore.PeerID, store Store) {
	self, err := o.IPFS().Key().Self(ctx)
	if err == nil {
		o.logger.Debug(fmt.Sprintf("%s: New peer '%s' connected to %s", self.ID(), p, store.Address().String()))
	} else {
		o.logger.Debug(fmt.Sprintf("New peer '%s' connected to %s", p, store.Address().String()))
	}

	_, err = o.exchangeHeads(ctx, p, store)

	if err != nil {
		o.logger.Error("unable to exchange heads", zap.Error(err))
		return
	}

	store.Emit(ctx, stores.NewEventNewPeer(p))
}

func (o *orbitDB) exchangeHeads(ctx context.Context, p p2pcore.PeerID, store Store) (iface.DirectChannel, error) {
	sharedKey := store.SharedKey()

	channel, err := o.getDirectConnection(ctx, p, sharedKey)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get a connection to peer")
	}

	o.logger.Debug(fmt.Sprintf("connecting to %s", p))

	err = channel.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to connect to peer")
	}

	o.logger.Debug(fmt.Sprintf("connected to %s", p))

	untypedHeads := store.OpLog().Heads().Slice()
	heads := make([]*entry.Entry, len(untypedHeads))
	for i := range untypedHeads {
		head, ok := untypedHeads[i].(*entry.Entry)
		if !ok {
			return nil, errors.New("unable to downcast head")
		}

		heads[i] = head
	}

	exchangedHeads := &exchangedHeads{
		Address: store.Address().String(),
		Heads:   heads,
	}

	exchangedHeadsBytes, err := json.Marshal(exchangedHeads)
	if err != nil {
		return nil, errors.Wrap(err, "unable to serialize heads to exchange")
	}

	if sharedKey != nil {
		exchangedHeadsBytes, err = sharedKey.Seal(exchangedHeadsBytes)
		if err != nil {
			return nil, errors.Wrap(err, "unable to encrypt payload")
		}
	}

	err = channel.Send(ctx, exchangedHeadsBytes)
	if err != nil {
		return nil, errors.Wrap(err, "unable to send heads on pubsub")
	}

	return channel, nil
}

func (o *orbitDB) watchOneOnOneMessage(ctx context.Context, channel iface.DirectChannel, sharedKey enc.SharedKey) {
	sub := channel.Subscribe(ctx)
	go func() {
		for evt := range sub {
			o.logger.Debug("received one on one message")

			switch e := evt.(type) {
			case *iface.EventPubSubPayload:
				heads := &exchangedHeads{}
				payload := e.Payload

				if sharedKey != nil {
					var err error

					payload, err = sharedKey.Open(payload)
					if err != nil {
						o.logger.Error("unable to decrypt payload", zap.Error(err))
						continue
					}
				}

				err := json.Unmarshal(payload, &heads)
				if err != nil {
					o.logger.Error("unable to unmarshal heads", zap.Error(err))
					continue
				}

				o.logger.Debug(fmt.Sprintf("%s: Received %d heads for '%s':", o.PeerID().String(), len(heads.Heads), heads.Address))
				store, ok := o.getStore(heads.Address)

				if !ok {
					o.logger.Debug("Heads from unknown store, skipping")
					continue
				}

				if len(heads.Heads) > 0 {
					untypedHeads := make([]ipfslog.Entry, len(heads.Heads))
					for i := range heads.Heads {
						untypedHeads[i] = heads.Heads[i]
					}

					if err := store.Sync(ctx, untypedHeads); err != nil {
						o.logger.Error("unable to sync heads", zap.Error(err))
					}
				}

			default:
				o.logger.Debug("unhandled event type")
			}
		}
	}()
}

var _ BaseOrbitDB = &orbitDB{}
