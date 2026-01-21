package replicator

import (
	"github.com/stateless-minds/go-ipfs-log/iface"
	cid "github.com/ipfs/go-cid"
)

type processItem interface {
	GetHash() cid.Cid
}

// process entry
type processEntry struct {
	entry iface.IPFSLogEntry
}

func newProcessEntry(entry iface.IPFSLogEntry) processItem {
	return &processEntry{
		entry: entry,
	}
}

func (p *processEntry) GetHash() cid.Cid {
	return p.entry.GetHash()
}

// process Hash
type processHash struct {
	hash cid.Cid
}

func newProcessHash(hash cid.Cid) processItem {
	return &processHash{
		hash: hash,
	}
}

func (p *processHash) GetHash() cid.Cid { return p.hash }

// A processQueue implements heap.Interface and holds Items.
// processQueue is not thread safe
type processQueue []processItem

func (pq *processQueue) Add(item processItem) {
	*pq = append(*pq, item)
}

func (pq *processQueue) Next() (item processItem) {
	old := *pq
	item = old[0]
	*pq = old[1:]
	return
}

func (pq processQueue) GetQueue() []processItem {
	return pq
}

func (pq processQueue) Len() int { return len(pq) }
