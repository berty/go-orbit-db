package orbitdb

import (
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
	ipfs2 "github.com/berty/go-orbit-db/accesscontroller/ipfs"
	"github.com/berty/go-orbit-db/address"
	"github.com/berty/go-orbit-db/ipfs"
	"github.com/berty/go-orbit-db/pubsub"
	"github.com/berty/go-orbit-db/stores"
	"github.com/berty/go-orbit-db/stores/eventlogstore"
	"github.com/berty/go-orbit-db/stores/kvstore"
	"github.com/berty/go-orbit-db/utils"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-leveldb"
	cbornode "github.com/ipfs/go-ipld-cbor"
	p2pcore "github.com/libp2p/go-libp2p-core"
	net "github.com/libp2p/go-libp2p-core/network"
	"github.com/pkg/errors"
	"log"

	"path"
	"strings"
)

func boolPtr(val bool) *bool {
	return &val
}

func stringPtr(val string) *string {
	return &val
}

type NewOrbitDBOptions struct {
	ID        *string
	PeerID    *p2pcore.PeerID
	Directory *string
	Keystore  *keystore.Keystore
	Cache     func(path string) (datastore.Datastore, error)
	Identity  *idp.Identity
}

type orbitDB struct {
	ipfs              ipfs.Services
	identity          *idp.Identity
	id                p2pcore.PeerID
	pubsub            pubsub.Interface
	keystore          *keystore.Keystore
	stores            map[string]stores.Interface
	directConnections map[string]net.Conn
	directory         string
	cacheConstructor  func(path string) (datastore.Datastore, error)
	cache             datastore.Datastore
}

func (o *orbitDB) Identity() *identityprovider.Identity {
	return o.identity
}

var defaultDirectory = "./orbitdb"

func newOrbitDB(ctx context.Context, is ipfs.Services, identity *idp.Identity, options *NewOrbitDBOptions) (orbitdb.OrbitDB, error) {
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
		options.Cache = func(path string) (datastore.Datastore, error) {
			return leveldb.NewDatastore(path, nil)
		}
	}

	if options.Directory == nil {
		options.Directory = &defaultDirectory
	}

	return &orbitDB{
		ipfs:             is,
		identity:         identity,
		id:               *options.PeerID,
		pubsub:           ps,
		cacheConstructor: options.Cache,
	}, nil
}

func NewOrbitDB(ctx context.Context, services ipfs.Services, options *NewOrbitDBOptions) (orbitdb.OrbitDB, error) {
	if services == nil {
		return nil, errors.New("services is a required argument")
	}

	k, err := services.Key().Self(ctx)
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
			return nil, errors.Wrap(err, "unable to create cacheConstructor data store")
		}

		ks, err := keystore.NewKeystore(ds)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create cacheConstructor keystore")
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

	return newOrbitDB(ctx, services, options.Identity, options)
}

func (o *orbitDB) Disconnect() {
	// FIXME: close keystore

	for k, store := range o.stores {
		_ = store.Close() // TODO: error handling
		delete(o.stores, k)
	}

	for k, conn := range o.directConnections {
		_ = conn.Close() // TODO: error handling
		delete(o.directConnections, k)
	}

	if o.pubsub != nil {
		o.pubsub.Disconnect()
	}

	o.stores = map[string]stores.Interface{}
}

func (o *orbitDB) Create(ctx context.Context, name string, storeType string, options *orbitdb.CreateDBOptions) (stores.Interface, error) {
	// The directory to look databases from can be passed in as an option
	if options.Directory == nil {
		options.Directory = &o.directory
	}

	// Create the database address
	dbAddress, err := o.DetermineAddress(ctx, name, storeType, &DetermineAddressOptions{})
	if err != nil {
		return nil, err
	}

	// Load the locally saved database information
	c, err := o.loadCache(o.directory, dbAddress)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load cacheConstructor")
	}

	o.cache = c

	// Check if we have the database locally
	haveDB := o.haveLocalData(c, dbAddress)

	if haveDB && !options.Overwrite {
		return nil, errors.New(fmt.Sprintf("database %s already exists", dbAddress))
	}

	// Save the database locally
	if err := o.addManifestToCache(o.directory, dbAddress); err != nil {
		return nil, errors.Wrap(err, "unable to add manifest to cacheConstructor")
	}

	// Open the database
	return o.Open(ctx, dbAddress.String(), options)
}

func (o *orbitDB) Log(ctx context.Context, address string, options *orbitdb.CreateDBOptions) (eventlogstore.OrbitDBEventLogStore, error) {
	if options == nil {
		options = &orbitdb.CreateDBOptions{}
	}

	options.Create = true
	options.StoreType = stringPtr("eventlog")
	store, err := o.Open(ctx, address, options)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open database")
	}

	logStore, ok := store.(eventlogstore.OrbitDBEventLogStore)
	if !ok {
		return nil, errors.New("unable to cast store to log")
	}

	return logStore, nil
}

func (o *orbitDB) KeyValue(ctx context.Context, address string, options *orbitdb.CreateDBOptions) (kvstore.OrbitDBKeyValue, error) {
	if options == nil {
		options = &orbitdb.CreateDBOptions{}
	}

	options.Create = true
	options.StoreType = stringPtr("keyvalue")

	store, err := o.Open(ctx, address, options)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open database")
	}

	kvStore, ok := store.(kvstore.OrbitDBKeyValue)
	if !ok {
		return nil, errors.New("unable to cast store to keyvalue")
	}

	return kvStore, nil
}

func (o *orbitDB) Open(ctx context.Context, dbAddress string, options *orbitdb.CreateDBOptions) (stores.Interface, error) {
	directory := o.directory
	if options.Directory != nil {
		directory = *options.Directory
	}

	if err := address.IsValid(directory); err != nil {
		if !options.Create {
			return nil, errors.New("'options.Create' set to 'false'. If you want to create a database, set 'options.Create' to 'true'")
		} else if options.Create && options.StoreType == nil {
			return nil, errors.New(fmt.Sprintf("database type not provided! Provide a type with 'options.type' (%s)", strings.Join(stores.StoreTypesNames(), "|")))
		} else {
			options.Overwrite = true

			return o.Create(ctx, dbAddress, *options.StoreType, options)
		}
	}

	parsedDBAddress, err := address.Parse(dbAddress)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse address")
	}

	cache, err := o.loadCache(directory, parsedDBAddress)
	if err != nil {
		return nil, errors.Wrap(err, "unable to acquire cacheConstructor")
	}

	haveDB := o.haveLocalData(cache, parsedDBAddress)
	if options.LocalOnly && !haveDB {
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

	return o.createStore(ctx, manifest.Type, parsedDBAddress, options)
}

type DetermineAddressOptions struct {
	OnlyHash         *bool
	AccessController accesscontroller.Interface
}

func (o *orbitDB) DetermineAddress(ctx context.Context, name string, storeType string, options *DetermineAddressOptions) (address.Address, error) {
	var err error

	if options.OnlyHash == nil {
		t := true
		options.OnlyHash = &t
	}

	if _, ok := stores.GetConstructor(storeType); !ok {
		return nil, errors.New("invalid database type")
	}

	if err := address.IsValid(name); err != nil {
		return nil, errors.Wrap(err, "given database name is an address, give only the name of the database")
	}

	options.AccessController, err = ipfs2.NewIPFSAccessController(ctx, o, &base.CreateAccessControllerOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize access controller")
	}

	accessControllerAddress, err := base.Create(ctx, o, options.AccessController.Type(), &base.CreateAccessControllerOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "unable to create access controller")
	}

	// Save the manifest to IPFS
	manifestHash, err := utils.CreateDBManifest(ctx, o.ipfs, name, storeType, accessControllerAddress.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "unable to save manifest on ipfs")
	}

	// Create the database address
	return address.Parse(path.Join("/orbitdb", manifestHash.String(), name))
}

func (o *orbitDB) loadCache(directory string, dbAddress address.Address) (datastore.Datastore, error) {
	db, err := o.cacheConstructor(path.Join(directory, dbAddress.GetPath()))
	if err != nil {
		return nil, errors.Wrap(err, "unable to load cache")
	}

	return db, nil
}

func (o *orbitDB) haveLocalData(c datastore.Datastore, dbAddress address.Address) bool {
	if c == nil {
		return false
	}

	data, err := c.Get(datastore.NewKey(path.Join(dbAddress.String(), "_manifest")))
	if err != nil {
		return false
	}

	return data != nil
}

func (o *orbitDB) addManifestToCache(directory string, dbAddress address.Address) error {
	cache, err := o.loadCache(directory, dbAddress)
	if err != nil {
		return errors.Wrap(err, "")
	}

	key := datastore.NewKey(path.Join(dbAddress.String(), "_manifest"))

	if err := cache.Put(key, []byte(dbAddress.GetRoot().String())); err != nil {
		return errors.Wrap(err, "unable to set cacheConstructor")
	}

	return nil
}

func (o *orbitDB) IPFS() ipfs.Services {
	return o.ipfs
}

func (o *orbitDB) createStore(ctx context.Context, storeType string, parsedDBAddress address.Address, options *orbitdb.CreateDBOptions) (stores.Interface, error) {
	var err error
	storeFunc, ok := stores.GetConstructor(storeType)
	if !ok {
		return nil, errors.New(fmt.Sprintf("store type %s is not supported", storeType))
	}

	var accessController accesscontroller.Interface
	if options.AccessControllerAddress != "" {
		c, err := cid.Parse(options.AccessControllerAddress)
		if err != nil {
			// TODO: check if a cid for options.AccessControllerAddress is more suited
			return nil, errors.Wrap(err, "unable to parse access controller address")
		}

		accessController, err = base.Resolve(ctx, o, options.AccessControllerAddress, accesscontroller.NewManifestParams(
			c, false, ""))
		if err != nil {
			return nil, errors.Wrap(err, "unable to acquire a access controller")
		}
	}

	cache, err := o.loadCache(o.directory, parsedDBAddress)
	if err != nil {
		return nil, errors.Wrap(err, "unable to acquire a cacheConstructor instance")
	}

	if options.Replicate == nil {
		options.Replicate = boolPtr(true)
	}

	options.AccessController = accessController
	options.Keystore = o.keystore
	options.Cache = cache

	identity := o.identity
	if options.Identity != nil {
		options.Identity = o.identity
	}

	store, err := storeFunc(ctx, o.ipfs, identity, parsedDBAddress, &stores.NewStoreOptions{
		AccessController: options.AccessController,
		Cache:            options.Cache,
		Replicate:        options.Replicate,
		Directory:        *options.Directory,
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate store")
	}

	store.Subscribe(o.storeListener(ctx))

	o.stores[parsedDBAddress.String()] = store

	return nil, nil
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

func (o *orbitDB) storeListener(ctx context.Context) chan stores.Event {
	c := make(chan stores.Event)

	go func() {
		for {
			select {
			case evt := <-c:
				o.storeListenerSwitch(ctx, evt)

			case <-ctx.Done():
				return
			}
		}
	}()

	return c
}

func (o *orbitDB) storeListenerSwitch(ctx context.Context, evt stores.Event) {
	switch evt.(type) {
	case *stores.EventClosed:
		e := evt.(*stores.EventClosed)
		err := o.onClose(ctx, e.Address.GetRoot())
		log.Printf("unable to perform onClose %v", err)

	case *stores.EventWrite:
		e := evt.(*stores.EventWrite)
		if len(e.Heads) == 0 {
			log.Printf("'heads' are not defined")
			return
		}

		if o.pubsub != nil {
			headsBytes, err := json.Marshal(e.Heads)
			if err != nil {
				log.Printf("unable to serialize heads %v", err)
				return
			}

			err = o.pubsub.Publish(ctx, e.Address.String(), headsBytes)
			if err != nil {
				log.Printf("unable to publish message on pubsub %v", err)
				return
			}
		}
	}
}

var _ orbitdb.OrbitDB = &orbitDB{}
