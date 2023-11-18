package basestore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-orbit-db/iface"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	files "github.com/ipfs/go-libipfs/files"
)

func SaveSnapshot(ctx context.Context, b iface.Store) (cid.Cid, error) {
	// @glouvigny: I'd rather use protobuf here but I decided to keep the
	// JS behavior for the sake of compatibility across implementations
	// TODO: avoid using `*entry.Entry`?

	unfinished := b.Replicator().GetQueue()

	oplog := b.OpLog()

	untypedEntries := oplog.Heads().Slice()
	entries := make([]*entry.Entry, len(untypedEntries))
	for i := range untypedEntries {
		castedEntry, ok := untypedEntries[i].(*entry.Entry)
		if !ok {
			return cid.Cid{}, fmt.Errorf("unable to downcast entry")
		}

		entries[i] = castedEntry
	}

	header, err := json.Marshal(&storeSnapshot{
		ID:    oplog.GetID(),
		Heads: entries,
		Size:  oplog.Len(),
		Type:  b.Type(),
	})

	if err != nil {
		return cid.Cid{}, fmt.Errorf("unable to serialize snapshot: %w", err)
	}

	headerSize := len(header)

	size := make([]byte, 2)
	binary.BigEndian.PutUint16(size, uint16(headerSize))
	rs := append(size, header...)

	for _, e := range oplog.GetEntries().Slice() {
		entryJSON, err := json.Marshal(e)

		if err != nil {
			return cid.Cid{}, fmt.Errorf("unable to serialize entry as JSON: %w", err)
		}

		size := make([]byte, 2)
		binary.BigEndian.PutUint16(size, uint16(len(entryJSON)))

		rs = append(rs, size...)
		rs = append(rs, entryJSON...)
	}

	rs = append(rs, 0)

	rsFileNode := files.NewBytesFile(rs)

	snapshotPath, err := b.IPFS().Unixfs().Add(ctx, rsFileNode)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("unable to save log data on store: %w", err)
	}

	err = b.Cache().Put(ctx, datastore.NewKey("snapshot"), []byte(snapshotPath.String()))
	if err != nil {
		return cid.Cid{}, fmt.Errorf("unable to add snapshot data to cache: %w", err)
	}

	unfinishedJSON, err := json.Marshal(unfinished)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("unable to marshal unfinished cids: %w", err)
	}

	err = b.Cache().Put(ctx, datastore.NewKey("queue"), unfinishedJSON)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("unable to add unfinished data to cache: %w", err)
	}

	return snapshotPath.RootCid(), nil
}
