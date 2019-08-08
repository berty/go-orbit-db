package replicator

import (
	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"github.com/ipfs/go-cid"
)

// EventLoadAdded An event triggered when entries have been added
type EventLoadAdded struct {
	Hash cid.Cid
}

// NewEventLoadAdded Creates a new EventLoadAdded event
func NewEventLoadAdded(h cid.Cid) *EventLoadAdded {
	return &EventLoadAdded{
		Hash: h,
	}
}

// EventLoadProgress An event triggered when entries have been loaded
type EventLoadProgress struct {
	ID            string
	Hash          cid.Cid
	Latest        *entry.Entry
	UnknownField4 interface{}
	BufferLength  int
}

// NewEventLoadProgress Creates a new EventLoadProgress event
func NewEventLoadProgress(id string, h cid.Cid, latest *entry.Entry, unknownField4 interface{}, bufferLength int) *EventLoadProgress {
	return &EventLoadProgress{
		ID:            id,
		Hash:          h,
		Latest:        latest,
		UnknownField4: unknownField4,
		BufferLength:  bufferLength,
	}
}

// EventLoadEnd An event triggered when load ended
type EventLoadEnd struct {
	Logs []*ipfslog.Log
}

// NewEventLoadEnd Creates a new EventLoadEnd event
func NewEventLoadEnd(logs []*ipfslog.Log) *EventLoadEnd {
	return &EventLoadEnd{
		Logs: logs,
	}
}
