package basestore

import (
	"context"
	"encoding/binary"
	"encoding/json"

	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-orbit-db/iface"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	files "github.com/ipfs/go-ipfs-files"
	"github.com/pkg/errors"
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
			return cid.Cid{}, errors.New("unable to downcast entry")
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
		return cid.Cid{}, errors.Wrap(err, "unable to serialize snapshot")
	}

	headerSize := len(header)

	size := make([]byte, 2)
	binary.BigEndian.PutUint16(size, uint16(headerSize))
	rs := append(size, header...)

	for _, e := range oplog.GetEntries().Slice() {
		entryJSON, err := json.Marshal(e)

		if err != nil {
			return cid.Cid{}, errors.Wrap(err, "unable to serialize entry as JSON")
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
		return cid.Cid{}, errors.Wrap(err, "unable to save log data on store")
	}

	err = b.Cache().Put(datastore.NewKey("snapshot"), []byte(snapshotPath.Cid().String()))
	if err != nil {
		return cid.Cid{}, errors.Wrap(err, "unable to add snapshot data to cache")
	}

	unfinishedJSON, err := json.Marshal(unfinished)
	if err != nil {
		return cid.Cid{}, errors.Wrap(err, "unable to marshal unfinished cids")
	}

	err = b.Cache().Put(datastore.NewKey("queue"), unfinishedJSON)
	if err != nil {
		return cid.Cid{}, errors.Wrap(err, "unable to add unfinished data to cache")
	}

	return snapshotPath.Cid(), nil
}
