package orbitdb

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"sync"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/identityprovider"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/io"
	"berty.tech/go-ipfs-log/keystore"
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/accesscontroller/base"
	"berty.tech/go-orbit-db/accesscontroller/ipfs"
	"berty.tech/go-orbit-db/accesscontroller/orbitdb"
	"berty.tech/go-orbit-db/accesscontroller/simple"
	"berty.tech/go-orbit-db/address"
	"berty.tech/go-orbit-db/cache"
	"berty.tech/go-orbit-db/cache/cacheleveldown"
	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/iface"
	_ "berty.tech/go-orbit-db/internal/buildconstraints" // fail for bad go version
	"berty.tech/go-orbit-db/pubsub"
	"berty.tech/go-orbit-db/pubsub/oneonone"
	"berty.tech/go-orbit-db/pubsub/peermonitor"
	"berty.tech/go-orbit-db/stores"
	"berty.tech/go-orbit-db/stores/eventlogstore"
	"berty.tech/go-orbit-db/stores/kvstore"
	"berty.tech/go-orbit-db/utils"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	cbornode "github.com/ipfs/go-ipld-cbor"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
	p2pcore "github.com/libp2p/go-libp2p-core"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// OrbitDB An alias of the type defined in the iface package
type OrbitDB = iface.OrbitDB

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

type exchangedHeads struct {
	Address string         `json:"address,omitempty"`
	Heads   []*entry.Entry `json:"heads,omitempty"`
}

func boolPtr(val bool) *bool {
	return &val
}

func stringPtr(val string) *string {
	return &val
}

// NewOrbitDBOptions Options for a new OrbitDB instance
type NewOrbitDBOptions struct {
	ID            *string
	PeerID        *p2pcore.PeerID
	Directory     *string
	Keystore      keystore.Interface
	Cache         cache.Interface
	Identity      *idp.Identity
	CloseKeystore func() error
}

type orbitDB struct {
	ipfs              coreapi.CoreAPI
	identity          *idp.Identity
	id                p2pcore.PeerID
	pubsub            pubsub.Interface
	keystore          keystore.Interface
	closeKeystore     func() error
	stores            map[string]Store
	directConnections map[p2pcore.PeerID]oneonone.Channel
	directory         string
	cache             cache.Interface
	lock              sync.RWMutex
}

func (o *orbitDB) Identity() *identityprovider.Identity {
	return o.identity
}

var defaultDirectory = "./orbitdb"

func newOrbitDB(ctx context.Context, is coreapi.CoreAPI, identity *idp.Identity, options *NewOrbitDBOptions) (OrbitDB, error) {
	if is == nil {
		return nil, errors.New("ipfs is a required argument")
	}

	if identity == nil {
		return nil, errors.New("identity is a required argument")
	}

	if options == nil {
		options = &NewOrbitDBOptions{}
	}

	k, err := is.Key().Self(ctx)
	if err != nil {
		return nil, err
	}

	ps, err := pubsub.NewPubSub(is, k.ID())
	if err != nil {
		return nil, err
	}

	if options.PeerID == nil {
		id := k.ID()
		options.PeerID = &id
	}

	if options.Cache == nil {
		options.Cache = cacheleveldown.New()
	}

	if options.Directory == nil {
		options.Directory = &defaultDirectory
	}

	return &orbitDB{
		ipfs:              is,
		identity:          identity,
		id:                *options.PeerID,
		pubsub:            ps,
		cache:             options.Cache,
		directory:         *options.Directory,
		stores:            map[string]Store{},
		directConnections: map[p2pcore.PeerID]oneonone.Channel{},
		closeKeystore:     options.CloseKeystore,
	}, nil
}

// NewOrbitDB Creates a new OrbitDB instance
func NewOrbitDB(ctx context.Context, ipfs coreapi.CoreAPI, options *NewOrbitDBOptions) (OrbitDB, error) {
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
		options.Directory = &defaultDirectory
	}

	if options.Keystore == nil {
		ds, err := leveldb.NewDatastore(path.Join(*options.Directory, id.String(), "/keystore"), nil)
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
	o.lock.RLock()
	c := o.cache
	s := o.stores
	dc := o.directConnections
	ps := o.pubsub
	closeKS := o.closeKeystore
	o.lock.RUnlock()

	for _, store := range s {
		err := store.Close()
		if err != nil {
			logger().Error("unable to close store", zap.Error(err))
		}
	}

	for _, conn := range dc {
		err := conn.Close()
		if err != nil {
			logger().Error("unable to close connection", zap.Error(err))
		}
	}

	if ps != nil {
		err := ps.Close()
		if err != nil {
			logger().Error("unable to close pubsub", zap.Error(err))
		}
	}

	o.lock.Lock()
	o.stores = map[string]Store{}
	o.directConnections = map[p2pcore.PeerID]oneonone.Channel{}
	o.pubsub = nil
	o.lock.Unlock()

	if err := c.Close(); err != nil {
		logger().Error("unable to close cache", zap.Error(err))
	}

	if closeKS != nil {
		if err := closeKS(); err != nil {
			logger().Error("unable to close key store", zap.Error(err))
		}
	}

	return nil
}

func (o *orbitDB) Create(ctx context.Context, name string, storeType string, options *CreateDBOptions) (Store, error) {
	logger().Debug("Create()")

	if options == nil {
		options = &CreateDBOptions{}
	}

	// The directory to look databases from can be passed in as an option
	if options.Directory == nil {
		options.Directory = &o.directory
	}

	logger().Debug(fmt.Sprintf("Creating database '%s' as %s in '%s'", name, storeType, o.directory))

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

	logger().Debug(fmt.Sprintf("Created database '%s'", dbAddress))

	// Open the database
	return o.Open(ctx, dbAddress.String(), options)
}

func (o *orbitDB) Log(ctx context.Context, address string, options *CreateDBOptions) (EventLogStore, error) {
	if options == nil {
		options = &CreateDBOptions{}
	}

	options.Create = boolPtr(true)
	options.StoreType = stringPtr("eventlog")
	store, err := o.Open(ctx, address, options)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open database")
	}

	logStore, ok := store.(EventLogStore)
	if !ok {
		return nil, errors.New("unable to cast store to log")
	}

	return logStore, nil
}

func (o *orbitDB) KeyValue(ctx context.Context, address string, options *CreateDBOptions) (KeyValueStore, error) {
	if options == nil {
		options = &CreateDBOptions{}
	}

	options.Create = boolPtr(true)
	options.StoreType = stringPtr("keyvalue")

	store, err := o.Open(ctx, address, options)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open database")
	}

	kvStore, ok := store.(KeyValueStore)
	if !ok {
		return nil, errors.New("unable to cast store to keyvalue")
	}

	return kvStore, nil
}

func (o *orbitDB) Open(ctx context.Context, dbAddress string, options *CreateDBOptions) (Store, error) {
	logger().Debug("Open()")

	if options == nil {
		options = &CreateDBOptions{}
	}

	if options.LocalOnly == nil {
		options.LocalOnly = boolPtr(false)
	}

	if options.Create == nil {
		options.Create = boolPtr(false)
	}

	logger().Debug("Open database ", zap.String("dbAddress", dbAddress))

	directory := o.directory
	if options.Directory != nil {
		directory = *options.Directory
	}

	logger().Debug("Look from ", zap.String("directory", directory))

	if err := address.IsValid(dbAddress); err != nil {
		if !*options.Create {
			return nil, errors.New("'options.Create' set to 'false'. If you want to create a database, set 'options.Create' to 'true'")
		} else if *options.Create && (options.StoreType == nil || *options.StoreType == "") {
			return nil, errors.New(fmt.Sprintf("database type not provided! Provide a type with 'options.StoreType' (%s)", strings.Join(stores.StoreTypesNames(), "|")))
		} else {
			logger().Debug(fmt.Sprintf("Not a valid OrbitDB address '%s', creating the database", dbAddress))

			options.Overwrite = boolPtr(true)

			return o.Create(ctx, dbAddress, *options.StoreType, options)
		}
	}
	logger().Debug(fmt.Sprintf("address '%s' is valid", dbAddress))

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

	manifestNode, err := io.ReadCBOR(ctx, o.ipfs, parsedDBAddress.GetRoot())
	if err != nil {
		return nil, errors.Wrap(err, "unable to fetch database manifest")
	}

	manifest := &utils.Manifest{}
	if err := cbornode.DecodeInto(manifestNode.RawData(), manifest); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal manifest")
	}

	logger().Debug("Creating store instance")

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

	if _, ok := stores.GetConstructor(storeType); !ok {
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

	accessControllerAddress, err := acbase.Create(ctx, o, options.AccessController.GetType(), options.AccessController)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create access controller")
	}

	// Save the manifest to IPFS
	manifestHash, err := utils.CreateDBManifest(ctx, o.ipfs, name, storeType, accessControllerAddress.String())
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
		logger().Debug("haveLocalData: no cache provided")
		return false
	}

	cacheKey := datastore.NewKey(path.Join(dbAddress.String(), "_manifest"))

	data, err := c.Get(cacheKey)
	if err != nil {
		if err != datastore.ErrNotFound {
			logger().Error("haveLocalData: error while getting value from cache", zap.Error(err))
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

func (o *orbitDB) IPFS() coreapi.CoreAPI {
	return o.ipfs
}

func (o *orbitDB) createStore(ctx context.Context, storeType string, parsedDBAddress address.Address, options *CreateDBOptions) (Store, error) {
	var err error
	storeFunc, ok := stores.GetConstructor(storeType)
	if !ok {
		return nil, errors.New(fmt.Sprintf("store type %s is not supported", storeType))
	}

	var accessController accesscontroller.Interface
	options.AccessControllerAddress = strings.TrimPrefix(options.AccessControllerAddress, "/ipfs/")

	if options.AccessControllerAddress != "" {
		logger().Debug(fmt.Sprintf("Access controller address is %s", options.AccessControllerAddress))

		c, _ := cid.Decode(options.AccessControllerAddress)

		ac := options.AccessController
		if ac == nil {
			ac = accesscontroller.NewEmptyManifestParams()
		} else {
			ac = accesscontroller.CloneManifestParams(options.AccessController)
		}
		ac.SetAddress(c)

		accessController, err = acbase.Resolve(ctx, o, options.AccessControllerAddress, ac)
		if err != nil {
			return nil, errors.Wrap(err, "unable to acquire an access controller")
		}
	}

	logger().Debug(fmt.Sprintf("loading cache for db %s", parsedDBAddress.String()))

	c, err := o.loadCache(o.directory, parsedDBAddress)
	if err != nil {
		return nil, errors.Wrap(err, "unable to acquire a cache instance")
	}

	if options.Replicate == nil {
		options.Replicate = boolPtr(true)
	}

	//options.AccessController = accessController
	options.Keystore = o.keystore
	options.Cache = c

	identity := o.identity
	if options.Identity != nil {
		identity = options.Identity
	}

	if options.Directory == nil {
		options.Directory = &o.directory
	}

	store, err := storeFunc(ctx, o.ipfs, identity, parsedDBAddress, &iface.NewStoreOptions{
		AccessController: accessController,
		Cache:            options.Cache,
		Replicate:        options.Replicate,
		Directory:        *options.Directory,
		CacheDestroy:     func() error { return o.cache.Destroy(o.directory, parsedDBAddress) },
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate store")
	}

	o.storeListener(ctx, store)

	o.lock.Lock()
	o.stores[parsedDBAddress.String()] = store
	o.lock.Unlock()

	// Subscribe to pubsub to get updates from peers,
	// this is what hooks us into the message propagation layer
	// and the p2p network
	o.lock.RLock()
	ps := o.pubsub
	o.lock.RUnlock()
	if *options.Replicate && ps != nil {
		sub, err := ps.Subscribe(ctx, parsedDBAddress.String())
		if err != nil {
			return nil, errors.Wrap(err, "unable to subscribe to pubsub")
		}

		o.pubSubChanListener(ctx, sub, parsedDBAddress)
	}

	return store, nil
}

func (o *orbitDB) onClose(ctx context.Context, addr cid.Cid) error {
	// Unsubscribe from pubsub
	o.lock.RLock()
	ps := o.pubsub
	o.lock.RUnlock()
	if ps != nil {
		if err := ps.Unsubscribe(addr.String()); err != nil {
			return errors.Wrap(err, "unable to unsubscribe from pubsub")
		}
	}

	o.lock.Lock()
	delete(o.stores, addr.String())
	o.lock.Unlock()

	return nil
}

func (o *orbitDB) storeListener(ctx context.Context, store Store) {
	go store.Subscribe(ctx, func(evt events.Event) {
		switch evt.(type) {
		case *stores.EventClosed:
			logger().Debug("received stores.close event")

			e := evt.(*stores.EventClosed)
			err := o.onClose(ctx, e.Address.GetRoot())
			logger().Debug(fmt.Sprintf("unable to perform onClose %v", err))

		case *stores.EventWrite:
			logger().Debug("received stores.write event")
			e := evt.(*stores.EventWrite)
			if len(e.Heads) == 0 {
				logger().Debug(fmt.Sprintf("'heads' are not defined"))
				return
			}

			o.lock.RLock()
			ps := o.pubsub
			o.lock.RUnlock()

			if ps != nil {
				headsBytes, err := json.Marshal(e.Heads)
				if err != nil {
					logger().Debug(fmt.Sprintf("unable to serialize heads %v", err))
					return
				}

				err = ps.Publish(ctx, e.Address.String(), headsBytes)
				if err != nil {
					logger().Debug(fmt.Sprintf("unable to publish message on pubsub %v", err))
					return
				}

				logger().Debug("stores.write event: published event on pub sub")
			}
		}
	})
}

func (o *orbitDB) pubSubChanListener(ctx context.Context, sub pubsub.Subscription, addr address.Address) {
	go sub.Subscribe(ctx, func(e events.Event) {
		logger().Debug("Got pub sub message")
		switch e.(type) {
		case *pubsub.MessageEvent:
			evt := e.(*pubsub.MessageEvent)

			addr := evt.Topic

			o.lock.RLock()
			store, ok := o.stores[addr]
			o.lock.RUnlock()

			if !ok {
				logger().Error(fmt.Sprintf("unable to find store for address %s", addr))
				return
			}

			headsEntriesBytes := evt.Content
			var headsEntries []*entry.Entry

			err := json.Unmarshal(headsEntriesBytes, &headsEntries)
			if err != nil {
				logger().Error("unable to unmarshal head entries")
			}

			if len(headsEntries) == 0 {
				logger().Debug(fmt.Sprintf("Nothing to synchronize for %s:", addr))
			}

			logger().Debug(fmt.Sprintf("Received %d heads for %s:", len(headsEntries), addr))

			entries := make([]ipfslog.Entry, len(headsEntries))
			for i := range headsEntries {
				entries[i] = headsEntries[i]
			}

			if err := store.Sync(ctx, entries); err != nil {
				logger().Debug(fmt.Sprintf("Error while syncing heads for %s:", addr))
			}
		case *peermonitor.EventPeerJoin:
			evt := e.(*peermonitor.EventPeerJoin)
			o.onNewPeerJoined(ctx, evt.Peer, addr)
			logger().Debug(fmt.Sprintf("peer %s joined from %s self is %s", evt.Peer.String(), addr, o.id))

		case *peermonitor.EventPeerLeave:
			evt := e.(*peermonitor.EventPeerLeave)
			logger().Debug(fmt.Sprintf("peer %s left from %s self is %s", evt.Peer.String(), addr, o.id))

		default:
			logger().Debug("unhandled event, can't match type")
		}
	})
}

func (o *orbitDB) onNewPeerJoined(ctx context.Context, p p2pcore.PeerID, addr address.Address) {
	self, err := o.ipfs.Key().Self(ctx)
	if err == nil {
		logger().Debug(fmt.Sprintf("%s: New peer '%s' connected to %s", self.ID(), p, addr.String()))
	} else {
		logger().Debug(fmt.Sprintf("New peer '%s' connected to %s", p, addr.String()))
	}

	_, err = o.exchangeHeads(ctx, p, addr)

	if err != nil {
		logger().Error("unable to exchange heads", zap.Error(err))
		return
	}

	o.lock.RLock()
	store, ok := o.stores[addr.String()]
	o.lock.RUnlock()

	if !ok {
		logger().Error(fmt.Sprintf("unable to get store for address %s", addr.String()))
		return
	}

	store.Emit(stores.NewEventNewPeer(p))
}

func (o *orbitDB) exchangeHeads(ctx context.Context, p p2pcore.PeerID, addr address.Address) (oneonone.Channel, error) {
	o.lock.RLock()
	store, ok := o.stores[addr.String()]
	o.lock.RUnlock()

	if !ok {
		return nil, errors.New(fmt.Sprintf("unable to get store for address %s", addr.String()))
	}

	channel, err := o.getDirectConnection(ctx, p)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get a connection to peer")
	}

	logger().Debug(fmt.Sprintf("connecting to %s", p))

	err = channel.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to connect to peer")
	}

	logger().Debug(fmt.Sprintf("connected to %s", p))

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
		Address: addr.String(),
		Heads:   heads,
	}

	exchangedHeadsBytes, err := json.Marshal(exchangedHeads)
	if err != nil {
		return nil, errors.Wrap(err, "unable to serialize heads to exchange")
	}

	err = channel.Send(ctx, exchangedHeadsBytes)
	if err != nil {
		return nil, errors.Wrap(err, "unable to send heads on pubsub")
	}

	return channel, nil
}

func (o *orbitDB) watchOneOnOneMessage(ctx context.Context, channel oneonone.Channel) {
	go channel.Subscribe(ctx, func(evt events.Event) {
		logger().Debug("received one on one message")

		switch evt.(type) {
		case *oneonone.EventMessage:
			e := evt.(*oneonone.EventMessage)

			heads := &exchangedHeads{}
			err := json.Unmarshal(e.Payload, &heads)
			if err != nil {
				logger().Error("unable to unmarshal heads", zap.Error(err))
			}

			logger().Debug(fmt.Sprintf("%s: Received %d heads for '%s':", o.id.String(), len(heads.Heads), heads.Address))
			o.lock.RLock()
			store, ok := o.stores[heads.Address]
			o.lock.RUnlock()

			if !ok {
				logger().Debug("Heads from unknown store, skipping")
				return
			}

			if len(heads.Heads) > 0 {
				untypedHeads := make([]ipfslog.Entry, len(heads.Heads))
				for i := range heads.Heads {
					untypedHeads[i] = heads.Heads[i]
				}

				if err := store.Sync(ctx, untypedHeads); err != nil {
					logger().Error("unable to sync heads", zap.Error(err))
				}
			}

		default:
			logger().Debug("unhandled event type")
		}
	})
}

func (o *orbitDB) getDirectConnection(ctx context.Context, peerID p2pcore.PeerID) (oneonone.Channel, error) {
	o.lock.RLock()
	conn, ok := o.directConnections[peerID]
	o.lock.RUnlock()

	if ok {
		return conn, nil
	}

	channel, err := oneonone.NewChannel(ctx, o.ipfs, peerID)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create a direct connection with peer")
	}

	o.lock.Lock()
	o.directConnections[peerID] = channel
	o.lock.Unlock()

	o.watchOneOnOneMessage(ctx, channel)

	return channel, nil
}

var _ OrbitDB = &orbitDB{}

func init() {
	stores.RegisterStore("eventlog", eventlogstore.NewOrbitDBEventLogStore)
	stores.RegisterStore("keyvalue", kvstore.NewOrbitDBKeyValue)

	_ = acbase.AddAccessController(ipfs.NewIPFSAccessController)
	_ = acbase.AddAccessController(orbitdb.NewOrbitDBAccessController)
	_ = acbase.AddAccessController(simple.NewSimpleAccessController)
}
