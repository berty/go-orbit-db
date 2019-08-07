package replicator

import (
	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"github.com/berty/go-orbit-db/events"
	"github.com/ipfs/go-cid"
)

type Event events.Event

type EventLoadAdded struct {
	Hash cid.Cid
}

func NewEventLoadAdded(h cid.Cid) *EventLoadAdded {
	return &EventLoadAdded{
		Hash: h,
	}
}

type EventLoadProgress struct {
	ID            string
	Hash          cid.Cid
	Latest        *entry.Entry
	UnknownField4 interface{}
	BufferLength  int
}

func NewEventLoadProgress(id string, h cid.Cid, latest *entry.Entry, unknownField4 interface{}, bufferLength int) *EventLoadProgress {
	return &EventLoadProgress{
		ID:            id,
		Hash:          h,
		Latest:        latest,
		UnknownField4: unknownField4,
		BufferLength:  bufferLength,
	}
}

type EventLoadEnd struct {
	Logs []*ipfslog.Log
}

func NewEventLoadEnd(logs []*ipfslog.Log) *EventLoadEnd {
	return &EventLoadEnd{
		Logs: logs,
	}
}
