// orbitdb implements the OrbitDB interface
package orbitdb

import (
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/identityprovider"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/io"
	"berty.tech/go-ipfs-log/keystore"
	"context"
	"encoding/json"
	"fmt"
	"github.com/berty/go-orbit-db"
	"github.com/berty/go-orbit-db/accesscontroller"
	"github.com/berty/go-orbit-db/accesscontroller/base"
	ipfsAccessController "github.com/berty/go-orbit-db/accesscontroller/ipfs"
	"github.com/berty/go-orbit-db/address"
	"github.com/berty/go-orbit-db/cache"
	"github.com/berty/go-orbit-db/cache/cacheleveldown"
	"github.com/berty/go-orbit-db/events"
	"github.com/berty/go-orbit-db/pubsub"
	"github.com/berty/go-orbit-db/pubsub/oneonone"
	"github.com/berty/go-orbit-db/pubsub/peermonitor"
	"github.com/berty/go-orbit-db/stores"
	"github.com/berty/go-orbit-db/stores/eventlogstore"
	"github.com/berty/go-orbit-db/stores/kvstore"
	"github.com/berty/go-orbit-db/utils"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-leveldb"
	cbornode "github.com/ipfs/go-ipld-cbor"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
	p2pcore "github.com/libp2p/go-libp2p-core"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"path"
	"strings"
)

func boolPtr(val bool) *bool {
	return &val
}

func stringPtr(val string) *string {
	return &val
}

// NewOrbitDBOptions Options for a new OrbitDB instance
type NewOrbitDBOptions struct {
	ID        *string
	PeerID    *p2pcore.PeerID
	Directory *string
	Keystore  *keystore.Keystore
	Cache     cache.Interface
	Identity  *idp.Identity
}

type orbitDB struct {
	ipfs              coreapi.CoreAPI
	identity          *idp.Identity
	id                p2pcore.PeerID
	pubsub            pubsub.Interface
	keystore          *keystore.Keystore
	stores            map[string]orbitdb.Store
	directConnections map[p2pcore.PeerID]oneonone.Channel
	directory         string
	cache             cache.Interface
}

func (o *orbitDB) Identity() *identityprovider.Identity {
	return o.identity
}

var defaultDirectory = "./orbitdb"

func newOrbitDB(ctx context.Context, is coreapi.CoreAPI, identity *idp.Identity, options *NewOrbitDBOptions) (orbitdb.OrbitDB, error) {
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
		stores:            map[string]orbitdb.Store{},
		directConnections: map[p2pcore.PeerID]oneonone.Channel{},
	}, nil
}

// NewOrbitDB Creates a new OrbitDB instance
func NewOrbitDB(ctx context.Context, ipfs coreapi.CoreAPI, options *NewOrbitDBOptions) (orbitdb.OrbitDB, error) {
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
	// FIXME: close keystore

	for k, store := range o.stores {
		err := store.Close()
		if err != nil {
			logger().Error("unable to close store", zap.Error(err))
		}
		delete(o.stores, k)
	}

	for k, conn := range o.directConnections {
		err := conn.Close()
		if err != nil {
			logger().Error("unable to close connection", zap.Error(err))
		}
		delete(o.directConnections, k)
	}

	if o.pubsub != nil {
		err := o.pubsub.Close()
		if err != nil {
			logger().Error("unable to close pubsub", zap.Error(err))
		}
	}

	o.stores = map[string]orbitdb.Store{}

	err := o.cache.Close()
	if err != nil {
		logger().Error("unable to close cache", zap.Error(err))
	}

	return nil
}

func (o *orbitDB) Create(ctx context.Context, name string, storeType string, options *orbitdb.CreateDBOptions) (orbitdb.Store, error) {
	logger().Debug("Create()")

	if options == nil {
		options = &orbitdb.CreateDBOptions{}
	}

	// The directory to look databases from can be passed in as an option
	if options.Directory == nil {
		options.Directory = &o.directory
	}

	logger().Debug(fmt.Sprintf("Creating database '%s' as %s in '%s'", name, storeType, o.directory))

	// Create the database address
	dbAddress, err := o.DetermineAddress(ctx, name, storeType, &orbitdb.DetermineAddressOptions{AccessController: options.AccessController})
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

func (o *orbitDB) Log(ctx context.Context, address string, options *orbitdb.CreateDBOptions) (orbitdb.EventLogStore, error) {
	if options == nil {
		options = &orbitdb.CreateDBOptions{}
	}

	options.Create = boolPtr(true)
	options.StoreType = stringPtr("eventlog")
	store, err := o.Open(ctx, address, options)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open database")
	}

	logStore, ok := store.(orbitdb.EventLogStore)
	if !ok {
		return nil, errors.New("unable to cast store to log")
	}

	return logStore, nil
}

func (o *orbitDB) KeyValue(ctx context.Context, address string, options *orbitdb.CreateDBOptions) (orbitdb.KeyValueStore, error) {
	if options == nil {
		options = &orbitdb.CreateDBOptions{}
	}

	options.Create = boolPtr(true)
	options.StoreType = stringPtr("keyvalue")

	store, err := o.Open(ctx, address, options)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open database")
	}

	kvStore, ok := store.(orbitdb.KeyValueStore)
	if !ok {
		return nil, errors.New("unable to cast store to keyvalue")
	}

	return kvStore, nil
}

func (o *orbitDB) Open(ctx context.Context, dbAddress string, options *orbitdb.CreateDBOptions) (orbitdb.Store, error) {
	logger().Debug("Open()")

	if options == nil {
		options = &orbitdb.CreateDBOptions{}
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

func (o *orbitDB) DetermineAddress(ctx context.Context, name string, storeType string, options *orbitdb.DetermineAddressOptions) (address.Address, error) {
	var err error

	if options == nil {
		options = &orbitdb.DetermineAddressOptions{}
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
		options.AccessController, err = ipfsAccessController.NewIPFSAccessController(ctx, o, &base.CreateAccessControllerOptions{})
		if err != nil {
			return nil, errors.Wrap(err, "unable to initialize access controller")
		}
	}

	accessControllerAddress, err := base.Create(ctx, o, options.AccessController.Type(), &base.CreateAccessControllerOptions{})
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

func (o *orbitDB) createStore(ctx context.Context, storeType string, parsedDBAddress address.Address, options *orbitdb.CreateDBOptions) (orbitdb.Store, error) {
	var err error
	storeFunc, ok := stores.GetConstructor(storeType)
	if !ok {
		return nil, errors.New(fmt.Sprintf("store type %s is not supported", storeType))
	}

	accessController := options.AccessController
	options.AccessControllerAddress = strings.TrimPrefix(options.AccessControllerAddress, "/ipfs/")

	if len(options.AccessControllerAddress) > 3 {
		logger().Debug(fmt.Sprintf("Access controller address is %s", options.AccessControllerAddress))

		c, err := cid.Decode(options.AccessControllerAddress)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("unable to parse access controller address (%s %d)", options.AccessControllerAddress, len(options.AccessControllerAddress)))
		}

		accessController, err = base.Resolve(ctx, o, options.AccessControllerAddress, accesscontroller.NewManifestParams(
			c, false, ""))
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

	options.AccessController = accessController
	options.Keystore = o.keystore
	options.Cache = c

	identity := o.identity
	if options.Identity != nil {
		identity = options.Identity
	}

	if options.Directory == nil {
		options.Directory = &o.directory
	}

	store, err := storeFunc(ctx, o.ipfs, identity, parsedDBAddress, &orbitdb.NewStoreOptions{
		AccessController: options.AccessController,
		Cache:            options.Cache,
		Replicate:        options.Replicate,
		Directory:        *options.Directory,
		CacheDestroy:     func() error { return o.cache.Destroy(o.directory, parsedDBAddress) },
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate store")
	}

	o.storeListener(ctx, store)

	o.stores[parsedDBAddress.String()] = store

	// Subscribe to pubsub to get updates from peers,
	// this is what hooks us into the message propagation layer
	// and the p2p network
	if *options.Replicate && o.pubsub != nil {
		sub, err := o.pubsub.Subscribe(ctx, parsedDBAddress.String())
		if err != nil {
			return nil, errors.Wrap(err, "unable to subscribe to pubsub")
		}

		o.pubSubChanListener(ctx, sub, parsedDBAddress)
	}

	return store, nil
}

func (o *orbitDB) onClose(ctx context.Context, addr cid.Cid) error {
	// Unsubscribe from pubsub
	if o.pubsub != nil {
		if err := o.pubsub.Unsubscribe(addr.String()); err != nil {
			return errors.Wrap(err, "unable to unsubscribe from pubsub")
		}
	}

	delete(o.stores, addr.String())

	return nil
}

func (o *orbitDB) storeListener(ctx context.Context, store orbitdb.Store) {
	go store.Subscribe(ctx, func (evt events.Event) {
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

			if o.pubsub != nil {
				headsBytes, err := json.Marshal(e.Heads)
				if err != nil {
					logger().Debug(fmt.Sprintf("unable to serialize heads %v", err))
					return
				}

				err = o.pubsub.Publish(ctx, e.Address.String(), headsBytes)
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
	go sub.Subscribe(ctx, func (e events.Event) {
		logger().Debug("Got pub sub message")
		switch e.(type) {
		case *pubsub.MessageEvent:
			evt := e.(*pubsub.MessageEvent)

			addr := evt.Topic

			store, ok := o.stores[addr]
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

			if err := store.Sync(ctx, headsEntries); err != nil {
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

	store, ok := o.stores[addr.String()]
	if !ok {
		logger().Error(fmt.Sprintf("unable to get store for address %s", addr.String()))
		return
	}

	store.Emit(stores.NewEventNewPeer(p))
}

func (o *orbitDB) exchangeHeads(ctx context.Context, p p2pcore.PeerID, addr address.Address) (oneonone.Channel, error) {
	store, ok := o.stores[addr.String()]
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

	exchangedHeads := &orbitdb.ExchangedHeads{
		Address: addr.String(),
		Heads:   store.OpLog().Heads().Slice(),
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
	go channel.Subscribe(ctx, func (evt events.Event) {
		logger().Debug("received one on one message")

		switch evt.(type) {
		case *oneonone.EventMessage:
			e := evt.(*oneonone.EventMessage)

			heads := &orbitdb.ExchangedHeads{}
			err := json.Unmarshal(e.Payload, &heads)
			if err != nil {
				logger().Error("unable to unmarshal heads", zap.Error(err))
			}

			logger().Debug(fmt.Sprintf("%s: Received %d heads for '%s':", o.id.String(), len(heads.Heads), heads.Address))
			store, ok := o.stores[heads.Address]
			if !ok {
				logger().Debug("Heads from unknown store, skipping")
				return
			}

			if len(heads.Heads) > 0 {
				if err := store.Sync(ctx, heads.Heads); err != nil {
					logger().Error("unable to sync heads", zap.Error(err))
				}
			}

		default:
			logger().Debug("unhandled event type")
		}
	})
}

func (o *orbitDB) getDirectConnection(ctx context.Context, peerID p2pcore.PeerID) (oneonone.Channel, error) {
	if conn, ok := o.directConnections[peerID]; ok {
		return conn, nil
	}

	channel, err := oneonone.NewChannel(ctx, o.ipfs, peerID)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create a direct connection with peer")
	}

	o.directConnections[peerID] = channel
	o.watchOneOnOneMessage(ctx, channel)

	return channel, nil
}

var _ orbitdb.OrbitDB = &orbitDB{}

func init() {
	stores.RegisterStore("eventlog", eventlogstore.NewOrbitDBEventLogStore)
	stores.RegisterStore("keyvalue", kvstore.NewOrbitDBKeyValue)
}
