package basestore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	ipfslog "berty.tech/go-ipfs-log"
	logac "berty.tech/go-ipfs-log/accesscontroller"
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/identityprovider"
	ifacelog "berty.tech/go-ipfs-log/iface"
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/accesscontroller/simple"
	"berty.tech/go-orbit-db/address"
	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores"
	"berty.tech/go-orbit-db/stores/operation"
	"berty.tech/go-orbit-db/stores/replicator"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	files "github.com/ipfs/go-ipfs-files"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
	"github.com/pkg/errors"
	otkv "go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// BaseStore The base of other stores
type BaseStore struct {
	emitters struct {
		evtWrite             event.Emitter
		evtReady             event.Emitter
		evtReplicateProgress event.Emitter
		evtLoad              event.Emitter
		evtLoadProgress      event.Emitter
		evtReplicated        event.Emitter
		evtReplicate         event.Emitter
	}

	id                string
	identity          *identityprovider.Identity
	address           address.Address
	dbName            string
	ipfs              coreapi.CoreAPI
	cache             datastore.Datastore
	access            accesscontroller.Interface
	oplog             ipfslog.Log
	replicator        replicator.Replicator
	index             iface.StoreIndex
	replicationStatus replicator.ReplicationInfo

	referenceCount int
	replicate      bool
	directory      string
	options        *iface.NewStoreOptions
	cacheDestroy   func() error

	muCache   sync.RWMutex
	muIndex   sync.RWMutex
	muJoining sync.Mutex
	sortFn    ipfslog.SortFn
	logger    *zap.Logger
	tracer    trace.Tracer
	ctx       context.Context
	cancel    context.CancelFunc

	// Deprecated: if possible don't use this, use EventBus() directly instead
	events.EventEmitter
}

func (b *BaseStore) DBName() string {
	return b.dbName
}

func (b *BaseStore) IPFS() coreapi.CoreAPI {
	return b.ipfs
}

func (b *BaseStore) Identity() *identityprovider.Identity {
	return b.identity
}

func (b *BaseStore) OpLog() ipfslog.Log {
	b.muIndex.RLock()
	defer b.muIndex.RUnlock()

	return b.oplog
}

func (b *BaseStore) AccessController() accesscontroller.Interface {
	return b.access
}

func (b *BaseStore) Replicator() replicator.Replicator {
	return b.replicator
}

func (b *BaseStore) Cache() datastore.Datastore {
	b.muCache.RLock()
	defer b.muCache.RUnlock()

	return b.cache
}

func (b *BaseStore) Logger() *zap.Logger {
	return b.logger
}

func (b *BaseStore) Tracer() trace.Tracer {
	return b.tracer
}

func (b *BaseStore) IO() ipfslog.IO {
	return b.options.IO
}

func (b *BaseStore) EventBus() event.Bus {
	return b.options.EventBus
}

func (b *BaseStore) Context() context.Context {
	return b.ctx
}

func (b *BaseStore) IsClosed() bool {
	select {
	case <-b.ctx.Done():
		return true
	default:
		return false
	}
}

// InitBaseStore Initializes the store base
func (b *BaseStore) InitBaseStore(ipfs coreapi.CoreAPI, identity *identityprovider.Identity, addr address.Address, options *iface.NewStoreOptions) error {
	var err error

	b.ctx, b.cancel = context.WithCancel(context.Background())

	if options == nil {
		options = &iface.NewStoreOptions{}
	}

	if options.EventBus == nil {
		options.EventBus = eventbus.NewBus()
	} else if err := b.SetBus(options.EventBus); err != nil {
		return fmt.Errorf("unable set event bus: %w", err)
	}

	if options.Logger == nil {
		options.Logger = zap.NewNop()
	}

	if options.Tracer == nil {
		options.Tracer = trace.NewNoopTracerProvider().Tracer("")
	}

	if identity == nil {
		return fmt.Errorf("identity required")
	}

	if err := b.generateEmitter(options.EventBus); err != nil {
		return err
	}

	b.logger = options.Logger
	b.tracer = options.Tracer
	b.id = addr.String()
	b.identity = identity
	b.address = addr
	if options.AccessController != nil {
		b.access = options.AccessController
	} else {
		manifestParams := accesscontroller.NewManifestParams(cid.Cid{}, true, "simple")
		manifestParams.SetAccess("write", []string{identity.ID})
		b.access, err = simple.NewSimpleAccessController(b.ctx, nil, manifestParams)

		if err != nil {
			return fmt.Errorf("unable to create simple access controller: %w", err)
		}
	}
	b.dbName = addr.GetPath()
	b.ipfs = ipfs
	b.replicationStatus = replicator.NewReplicationInfo()

	b.muCache.Lock()
	b.cache = options.Cache
	b.cacheDestroy = options.CacheDestroy
	b.sortFn = options.SortFn
	b.muCache.Unlock()

	b.muIndex.Lock()
	b.oplog, err = ipfslog.NewLog(ipfs, identity, &ipfslog.LogOptions{
		ID:               addr.String(),
		AccessController: b.AccessController(),
		SortFn:           b.sortFn,
		IO:               options.IO,
	})

	if err != nil {
		b.muIndex.Unlock()
		return fmt.Errorf("unable to instantiate an IPFS log")
	}

	if options.Index == nil {
		options.Index = NewBaseIndex
	}

	b.index = options.Index(b.Identity().PublicKey)
	b.muIndex.Unlock()

	b.replicator, err = replicator.NewReplicator(b, options.ReplicationConcurrency, &replicator.Options{
		Logger:   b.logger,
		EventBus: options.EventBus,
		Tracer:   b.tracer,
	})
	if err != nil {
		return fmt.Errorf("unable to init error: %w", err)
	}

	b.referenceCount = 64
	if options.ReferenceCount != nil {
		b.referenceCount = *options.ReferenceCount
	}

	// TODO: Doesn't seem to be used
	b.directory = "./orbitdb"
	if options.Directory != "" {
		b.directory = options.Directory
	}

	// TODO: Doesn't seem to be used
	b.replicate = true
	if options.Replicate != nil {
		b.replicate = *options.Replicate
	}

	b.options = options

	sub, err := b.replicator.EventBus().Subscribe(replicator.Events, eventbus.BufSize(128))
	if err != nil {
		return fmt.Errorf("unable to subscribe to replicator events: %w", err)
	}

	go func() {
		defer sub.Close()
		ctx, span := b.tracer.Start(b.ctx, "base-store-main-loop", trace.WithAttributes(otkv.String("store-address", b.Address().String())))
		defer span.End()

		var e interface{}
		for {
			select {
			case e = <-sub.Out():
			case <-ctx.Done():
				return
			}

			switch evt := e.(type) {
			case replicator.EventLoadAdded:
				maxTotal := 0
				if evt.Entry != nil && evt.Entry.GetClock().Defined() {
					maxTotal = evt.Entry.GetClock().GetTime()
				}

				b.recalculateReplicationMax(maxTotal)

				if err := b.emitters.evtReplicate.Emit(stores.NewEventReplicate(b.Address(), evt.Hash)); err != nil {
					b.logger.Warn("unable to emit event replicate", zap.Error(err))
				}

			case replicator.EventLoadEnd:
				span.AddEvent("replicator-load-end")

				// @FIXME(gfanton): should we run this in a goroutine ?
				b.replicationLoadComplete(ctx, evt.Logs)

			case replicator.EventLoadProgress:
				span.AddEvent("replicator-load-progress")

				//      @FIXME(gfanton): this currently doesn't work and wont emit replicate progress
				// 	previousProgress := b.ReplicationStatus().GetProgress()
				// 	previousMax := b.ReplicationStatus().GetMax()

				// 	maxTotal := evt.Entry.GetClock().GetTime()

				// 	b.recalculateReplicationStatus(maxTotal)
				// 	fmt.Printf("%d: after %d > %d || %d > %d\n", maxTotal,
				// 		b.ReplicationStatus().GetProgress(), previousProgress, b.ReplicationStatus().GetMax(), previousMax)
				// 	if b.ReplicationStatus().GetProgress() > previousProgress || b.ReplicationStatus().GetMax() > previousMax {
				// 		hash := evt.Entry.GetHash()
				// 		err := b.emitters.evtReplicateProgress.Emit(stores.NewEventReplicateProgress(b.Address(), hash, evt.Entry, b.ReplicationStatus()))
				// 		if err != nil {
				// 			b.logger.Warn("unable to emit event replicate progress", zap.Error(err))
				// 		}
				// 	}

				maxTotal := 0
				if evt.Entry != nil {
					maxTotal = evt.Entry.GetClock().GetTime()
				}

				b.recalculateReplicationStatus(maxTotal)
				hash := evt.Entry.GetHash()
				err := b.emitters.evtReplicateProgress.Emit(stores.NewEventReplicateProgress(b.Address(), hash, evt.Entry, b.ReplicationStatus()))
				if err != nil {
					b.logger.Warn("unable to emit event replicate progress", zap.Error(err))
				}

			}
		}
	}()

	return nil
}

func (b *BaseStore) Close() error {
	if b.IsClosed() {
		return nil
	}

	// Replicator teardown logic
	b.Replicator().Stop()

	// close emitters
	emitters := []event.Emitter{
		b.emitters.evtWrite, b.emitters.evtReady,
		b.emitters.evtReplicateProgress, b.emitters.evtLoad,
		b.emitters.evtReplicated, b.emitters.evtReplicate,
	}
	for _, emitter := range emitters {
		if err := emitter.Close(); err != nil {
			b.logger.Warn("unable to close emitter", zap.Error(err))
		}
	}

	// Reset replication statistics
	b.ReplicationStatus().Reset()

	err := b.Cache().Close()
	if err != nil {
		return fmt.Errorf("unable to close cache: %w", err)
	}

	b.cancel()

	return nil
}

func (b *BaseStore) Address() address.Address {
	return b.address
}

func (b *BaseStore) Index() iface.StoreIndex {
	b.muIndex.RLock()
	defer b.muIndex.RUnlock()

	return b.index
}

func (b *BaseStore) Type() string {
	return "store"
}

func (b *BaseStore) ReplicationStatus() replicator.ReplicationInfo {
	return b.replicationStatus
}

func (b *BaseStore) Drop() error {
	var err error
	if err = b.Close(); err != nil {
		return fmt.Errorf("unable to close store: %w", err)
	}

	err = b.cacheDestroy()
	if err != nil {
		return fmt.Errorf("unable to destroy cache: %w", err)
	}

	// TODO: Destroy cache? b.cache.Delete()

	// Reset
	b.muIndex.Lock()
	b.index = b.options.Index(b.Identity().PublicKey)
	b.oplog, err = ipfslog.NewLog(b.IPFS(), b.Identity(), &ipfslog.LogOptions{
		ID:               b.id,
		AccessController: b.AccessController(),
		SortFn:           b.SortFn(),
		IO:               b.options.IO,
	})
	b.muIndex.Unlock()

	if err != nil {
		return fmt.Errorf("unable to create log: %w", err)
	}

	b.muCache.Lock()
	b.cache = b.options.Cache
	b.muCache.Unlock()

	return nil
}

func (b *BaseStore) Load(ctx context.Context, amount int) error {
	ctx, span := b.tracer.Start(ctx, "store-load")
	defer span.End()

	if amount <= 0 && b.options.MaxHistory != nil {
		amount = *b.options.MaxHistory
	}

	var localHeads, remoteHeads []*entry.Entry
	localHeadsBytes, err := b.Cache().Get(ctx, datastore.NewKey("_localHeads"))
	if err != nil && err != datastore.ErrNotFound {
		span.AddEvent("local-heads-load-failed")
		return fmt.Errorf("unable to get local heads from cache: %w", err)
	}

	err = nil

	if localHeadsBytes != nil {
		span.AddEvent("local-heads-unmarshall")
		err = json.Unmarshal(localHeadsBytes, &localHeads)
		if err != nil {
			span.AddEvent("local-heads-unmarshall-failed")
			b.logger.Warn("unable to unmarshal cached local heads", zap.Error(err))
		}
		span.AddEvent("local-heads-unmarshalled")
	}

	remoteHeadsBytes, err := b.Cache().Get(ctx, datastore.NewKey("_remoteHeads"))
	if err != nil && err != datastore.ErrNotFound {
		span.AddEvent("remote-heads-load-failed")
		return fmt.Errorf("unable to get data from cache: %w", err)
	}

	err = nil

	if remoteHeadsBytes != nil {
		span.AddEvent("remote-heads-unmarshall")
		err = json.Unmarshal(remoteHeadsBytes, &remoteHeads)
		if err != nil {
			span.AddEvent("remote-heads-unmarshall-failed")
			return fmt.Errorf("unable to unmarshal cached remote heads: %w", err)
		}
		span.AddEvent("remote-heads-unmarshalled")
	}

	heads := append(localHeads, remoteHeads...)

	if len(heads) > 0 {
		headsForEvent := make([]ipfslog.Entry, len(heads))
		for i := range heads {
			headsForEvent[i] = heads[i]
		}

		if err := b.emitters.evtLoad.Emit(stores.NewEventLoad(b.Address(), headsForEvent)); err != nil {
			b.logger.Warn("unable to emit event load", zap.Error(err))
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(len(heads))

	// @FIXME(gfanton): chan progress should be created and close on ipfs-log
	progress := make(chan ifacelog.IPFSLogEntry)
	defer close(progress)
	go func() {
		for {
			var entry ifacelog.IPFSLogEntry
			select {
			case <-ctx.Done():
				return
			case entry = <-progress:
				if entry == nil {
					// should not happen
					return
				}
			}

			b.recalculateReplicationStatus(entry.GetClock().GetTime())
			evt := stores.NewEventLoadProgress(b.Address(), entry.GetHash(), entry, b.replicationStatus.GetProgress(), b.replicationStatus.GetMax())
			if err := b.emitters.evtLoadProgress.Emit(evt); err != nil {
				b.logger.Warn("unable to emit event load", zap.Error(err))
			}
		}
	}()

	for _, h := range heads {
		go func(h *entry.Entry) {
			ctx, span := b.tracer.Start(ctx, "store-handling-head", trace.WithAttributes(otkv.String("cid", h.GetHash().String())))
			b.muJoining.Lock()
			defer b.muJoining.Unlock()
			defer wg.Done()

			oplog := b.OpLog()

			span.AddEvent("store-head-loading")

			l, inErr := ipfslog.NewFromEntryHash(ctx, b.IPFS(), b.Identity(), h.GetHash(), &ipfslog.LogOptions{
				ID:               oplog.GetID(),
				AccessController: b.AccessController(),
				SortFn:           b.SortFn(),
				IO:               b.options.IO,
			}, &ipfslog.FetchOptions{
				Length:       &amount,
				Exclude:      oplog.GetEntries().Slice(),
				ProgressChan: progress,
			})

			if inErr != nil {
				span.AddEvent("store-head-loading-error")
				err = errors.Wrap(inErr, "unable to create log from entry hash")
				return
			}

			b.recalculateReplicationStatus(h.GetClock().GetTime())

			span.AddEvent("store-head-loaded")

			span.AddEvent("store-heads-joining")
			if _, inErr = oplog.Join(l, amount); inErr != nil {
				span.AddEvent("store-heads-joining-failed")
				// err = fmt.Errorf("unable to join log: %w", err)
				// TODO: log
				_ = inErr
			} else {
				span.AddEvent("store-heads-joined")
			}
		}(h)
	}

	wg.Wait()

	if err != nil {
		span.AddEvent("store-handling-head-error", trace.WithAttributes(otkv.String("error", err.Error())))
		return err
	}

	// Update the index
	if len(heads) > 0 {
		span.AddEvent("store-index-updating")
		if err := b.updateIndex(ctx); err != nil {
			span.AddEvent("store-index-updating-error", trace.WithAttributes(otkv.String("error", err.Error())))
			return fmt.Errorf("unable to update index: %w", err)
		}
		span.AddEvent("store-index-updated")
	}

	if err := b.emitters.evtReady.Emit(stores.NewEventReady(b.Address(), b.OpLog().Heads().Slice())); err != nil {
		return fmt.Errorf("unable to emit event ready: %w", err)
	}

	return nil
}

func (b *BaseStore) Sync(ctx context.Context, heads []ipfslog.Entry) error {
	ctx, span := b.tracer.Start(ctx, "store-sync")
	defer span.End()

	if len(heads) == 0 {
		return nil
	}

	for _, h := range heads {
		if h == nil {
			b.Logger().Debug("warning: Given input entry was 'null'.")
			continue
		}

		if h.GetNext() == nil {
			h.SetNext([]cid.Cid{})
		}

		if h.GetRefs() == nil {
			h.SetRefs([]cid.Cid{})
		}

		identityProvider := b.Identity().Provider
		if identityProvider == nil {
			return fmt.Errorf("identity-provider is required, cannot verify entry")
		}

		canAppend := b.AccessController().CanAppend(h, identityProvider, &CanAppendContext{log: b.OpLog()})
		if canAppend != nil {
			span.AddEvent("store-sync-cant-append", trace.WithAttributes(otkv.String("error", canAppend.Error())))
			b.Logger().Debug("warning: Given input entry is not allowed in this log and was discarded (no write access)", zap.Error(canAppend))
			continue
		}

		hash, err := b.IO().Write(ctx, b.IPFS(), h, nil)
		if err != nil {
			span.AddEvent("store-sync-cant-write", trace.WithAttributes(otkv.String("error", err.Error())))
			return fmt.Errorf("unable to write entry on dag: %w", err)
		}

		if hash.String() != h.GetHash().String() {
			span.AddEvent("store-sync-cant-verify-hash")
			return fmt.Errorf("WARNING! Head hash didn't match the contents")
		}

		span.AddEvent("store-sync-head-verified")
	}

	go b.Replicator().Load(ctx, heads)

	return nil
}

func (b *BaseStore) LoadMoreFrom(ctx context.Context, amount uint, entries []ipfslog.Entry) {
	b.Replicator().Load(ctx, entries)
	// TODO: can this return an error?
}

type storeSnapshot struct {
	ID    string         `json:"id,omitempty"`
	Heads []*entry.Entry `json:"heads,omitempty"`
	Size  int            `json:"size,omitempty"`
	Type  string         `json:"type,omitempty"`
}

func (b *BaseStore) LoadFromSnapshot(ctx context.Context) error {
	b.muJoining.Lock()
	defer b.muJoining.Unlock()

	ctx, span := b.tracer.Start(ctx, "load-from-snapshot")
	defer span.End()

	if err := b.emitters.evtLoad.Emit(stores.NewEventLoad(b.Address(), nil)); err != nil {
		b.logger.Warn("unable to emit event load event", zap.Error(err))
	}

	queueJSON, err := b.Cache().Get(ctx, datastore.NewKey("queue"))
	if err != nil && err != datastore.ErrNotFound {
		return fmt.Errorf("unable to get value from cache: %w", err)
	}

	if err != datastore.ErrNotFound {
		var queue []cid.Cid

		var entries []ipfslog.Entry

		if err := json.Unmarshal(queueJSON, &queue); err != nil {
			return fmt.Errorf("unable to deserialize queued CIDs: %w", err)
		}

		for _, h := range queue {
			entries = append(entries, &entry.Entry{Hash: h})
		}

		if err := b.Sync(ctx, entries); err != nil {
			return fmt.Errorf("unable to sync queued CIDs: %w", err)
		}
	}

	snapshot, err := b.Cache().Get(ctx, datastore.NewKey("snapshot"))
	if err == datastore.ErrNotFound {
		return fmt.Errorf("not found: %w", err)
	}

	if err != nil {
		return fmt.Errorf("unable to get value from cache: %w", err)
	}

	b.Logger().Debug("loading snapshot from path", zap.String("snapshot", string(snapshot)))

	resNode, err := b.IPFS().Unixfs().Get(ctx, path.New(string(snapshot)))
	if err != nil {
		return fmt.Errorf("unable to get snapshot from ipfs: %w", err)
	}

	res, ok := resNode.(files.File)
	if !ok {
		return fmt.Errorf("unable to cast fetched data as a file")
	}

	headerLengthRaw := make([]byte, 2)
	if _, err := res.Read(headerLengthRaw); err != nil {
		return fmt.Errorf("unable to read from stream: %w", err)
	}

	headerLength := binary.BigEndian.Uint16(headerLengthRaw)
	header := &storeSnapshot{}
	headerRaw := make([]byte, headerLength)
	if _, err := res.Read(headerRaw); err != nil {
		return fmt.Errorf("unable to read from stream: %w", err)
	}

	if err := json.Unmarshal(headerRaw, &header); err != nil {
		return fmt.Errorf("unable to decode header from ipfs data: %w", err)
	}

	var entries []ipfslog.Entry
	maxClock := 0

	for i := 0; i < header.Size; i++ {
		entryLengthRaw := make([]byte, 2)
		if _, err := res.Read(entryLengthRaw); err != nil {
			return fmt.Errorf("unable to read from stream: %w", err)
		}

		entryLength := binary.BigEndian.Uint16(entryLengthRaw)
		e := &entry.Entry{}
		entryRaw := make([]byte, entryLength)

		if _, err := res.Read(entryRaw); err != nil {
			return fmt.Errorf("unable to read from stream: %w", err)
		}

		b.Logger().Debug(fmt.Sprintf("Entry raw: %s", string(entryRaw)))

		if err = json.Unmarshal(entryRaw, e); err != nil {
			return fmt.Errorf("unable to unmarshal entry from ipfs data: %w", err)
		}

		entries = append(entries, e)
		if maxClock < e.Clock.GetTime() {
			maxClock = e.Clock.GetTime()
		}
	}

	b.recalculateReplicationMax(maxClock)

	var headsCids []cid.Cid
	for _, h := range header.Heads {
		headsCids = append(headsCids, h.GetHash())
	}

	log, err := ipfslog.NewFromJSON(ctx, b.IPFS(), b.Identity(), &ipfslog.JSONLog{
		Heads: headsCids,
		ID:    header.ID,
	}, &ipfslog.LogOptions{
		Entries:          entry.NewOrderedMapFromEntries(entries),
		ID:               header.ID,
		AccessController: b.AccessController(),
		SortFn:           b.SortFn(),
		IO:               b.options.IO,
	}, &entry.FetchOptions{
		Length: intPtr(-1),
		// @FIXME(gfanton): should we increase this ?
		Timeout: time.Minute,
	})

	if err != nil {
		return fmt.Errorf("unable to load log: %w", err)
	}

	if _, err = b.OpLog().Join(log, -1); err != nil {
		return fmt.Errorf("unable to join log: %w", err)
	}

	if err := b.updateIndex(ctx); err != nil {
		return fmt.Errorf("unable to update index: %w", err)
	}

	return nil
}

func intPtr(i int) *int {
	return &i
}

func (b *BaseStore) AddOperation(ctx context.Context, op operation.Operation, onProgressCallback chan<- ipfslog.Entry) (ipfslog.Entry, error) {
	ctx, span := b.tracer.Start(ctx, "add-operation")
	defer span.End()

	data, err := op.Marshal()
	if err != nil {
		return nil, fmt.Errorf("unable to marshal operation: %w", err)
	}

	oplog := b.OpLog()

	e, err := oplog.Append(ctx, data, &ipfslog.AppendOptions{PointerCount: b.referenceCount})
	if err != nil {
		return nil, fmt.Errorf("unable to append data on log: %w", err)
	}

	b.recalculateReplicationStatus(e.GetClock().GetTime())

	marshaledEntry, err := json.Marshal([]ipfslog.Entry{e})
	if err != nil {
		return nil, fmt.Errorf("unable to marshal entry: %w", err)
	}

	err = b.Cache().Put(ctx, datastore.NewKey("_localHeads"), marshaledEntry)
	if err != nil {
		return nil, fmt.Errorf("unable to add data to cache: %w", err)
	}

	if err := b.updateIndex(ctx); err != nil {
		return nil, fmt.Errorf("unable to update index: %w", err)
	}

	if err := b.emitters.evtWrite.Emit(stores.NewEventWrite(b.Address(), e, oplog.Heads().Slice())); err != nil {
		b.logger.Warn("unable to emit event write", zap.Error(err))
	}

	if onProgressCallback != nil {
		onProgressCallback <- e
	}

	return e, nil
}

func (b *BaseStore) recalculateReplicationProgress() {
	max := b.ReplicationStatus().GetMax()
	if progress := b.ReplicationStatus().GetProgress() + 1; progress < max {
		max = progress
	}
	if opLogLen := b.OpLog().Len(); opLogLen > max {
		max = opLogLen

	}

	b.ReplicationStatus().SetProgress(max)
}

func (b *BaseStore) recalculateReplicationMax(max int) {
	if opLogLen := b.OpLog().Len(); opLogLen > max {
		max = opLogLen

	} else if replMax := b.ReplicationStatus().GetMax(); replMax > max {
		max = replMax
	}

	b.ReplicationStatus().SetMax(max)
}

func (b *BaseStore) recalculateReplicationStatus(maxTotal int) {
	b.recalculateReplicationMax(maxTotal)
	b.recalculateReplicationProgress()
}

func (b *BaseStore) updateIndex(ctx context.Context) error {
	_, span := b.tracer.Start(ctx, "update-index")
	defer span.End()

	if err := b.Index().UpdateIndex(b.OpLog(), []ipfslog.Entry{}); err != nil {
		return fmt.Errorf("unable to update index: %w", err)
	}

	return nil
}

func (b *BaseStore) generateEmitter(bus event.Bus) error {
	var err error

	if b.emitters.evtWrite, err = bus.Emitter(new(stores.EventWrite)); err != nil {
		return fmt.Errorf("unable to create EventWrite emitter: %w", err)
	}

	if b.emitters.evtReady, err = bus.Emitter(new(stores.EventReady)); err != nil {
		return fmt.Errorf("unable to create EventReady emitter: %w", err)
	}

	if b.emitters.evtReplicateProgress, err = bus.Emitter(new(stores.EventReplicateProgress)); err != nil {
		return fmt.Errorf("unable to create EventReplicateProgress emitter: %w", err)
	}

	if b.emitters.evtLoad, err = bus.Emitter(new(stores.EventLoad)); err != nil {
		return fmt.Errorf("unable to create EventLoad emitter: %w", err)
	}

	if b.emitters.evtLoadProgress, err = bus.Emitter(new(stores.EventLoadProgress)); err != nil {
		return fmt.Errorf("unable to create EventLoad emitter: %w", err)
	}

	if b.emitters.evtReplicated, err = bus.Emitter(new(stores.EventReplicated)); err != nil {
		return fmt.Errorf("unable to create EventReplicated emitter: %w", err)
	}

	if b.emitters.evtReplicate, err = bus.Emitter(new(stores.EventReplicate)); err != nil {
		return fmt.Errorf("unable to create EventReplicate emitter: %w", err)
	}

	return nil
}

func (b *BaseStore) replicationLoadComplete(ctx context.Context, logs []ipfslog.Log) {
	b.muJoining.Lock()
	defer b.muJoining.Unlock()

	oplog := b.OpLog()

	b.Logger().Debug("replication load complete")
	entries := []ipfslog.Entry{}
	for _, log := range logs {
		_, err := oplog.Join(log, -1)
		if err != nil {
			b.Logger().Error("unable to join logs", zap.Error(err))
			return
		}

		entries = append(entries, log.GetEntries().Slice()...)
	}

	err := b.updateIndex(ctx)
	if err != nil {
		b.Logger().Error("unable to update index", zap.Error(err))
		return
	}

	// only store heads that has been verified and merges
	heads := oplog.Heads()

	headsBytes, err := json.Marshal(heads.Slice())
	if err != nil {
		b.Logger().Error("unable to serialize heads cache", zap.Error(err))
		return
	}

	err = b.Cache().Put(ctx, datastore.NewKey("_remoteHeads"), headsBytes)
	if err != nil {
		b.Logger().Error("unable to update heads cache", zap.Error(err))
		return
	}

	if oplog.Len() > b.replicationStatus.GetProgress() {
		b.recalculateReplicationStatus(oplog.Len())
	}

	b.Logger().Debug(fmt.Sprintf("Saved heads %d", heads.Len()))

	// logger.debug(`<replicated>`)
	if err := b.emitters.evtReplicated.Emit(stores.NewEventReplicated(b.Address(), entries, len(logs))); err != nil {
		b.Logger().Warn("unable to emit event replicated", zap.Error(err))
	}
}

func (b *BaseStore) SortFn() ipfslog.SortFn {
	return b.sortFn
}

type CanAppendContext struct {
	log ipfslog.Log
}

func (c *CanAppendContext) GetLogEntries() []logac.LogEntry {
	logEntries := c.log.GetEntries().Slice()

	var entries = make([]logac.LogEntry, len(logEntries))
	for i := range logEntries {
		entries[i] = logEntries[i]
	}

	return entries
}

var _ iface.Store = &BaseStore{}
