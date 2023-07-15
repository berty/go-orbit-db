package replicator

import (
	ipfslog "github.com/stateless-minds/go-ipfs-log"
	cid "github.com/ipfs/go-cid"
)

var Events = []interface{}{
	new(EventLoadEnd),
	new(EventLoadAdded),
	new(EventLoadProgress),
}

// EventLoadAdded An event triggered when entries have been added
type EventLoadAdded struct {
	Entry ipfslog.Entry
	Hash  cid.Cid
}

// NewEventLoadAdded Creates a new EventLoadAdded event
func NewEventLoadAdded(h cid.Cid, entry ipfslog.Entry) EventLoadAdded {
	return EventLoadAdded{
		Entry: entry,
		Hash:  h,
	}
}

// EventLoadProgress An event triggered when entries have been loaded
type EventLoadProgress struct {
	Entry ipfslog.Entry
}

// NewEventLoadProgress Creates a new EventLoadProgress event
func NewEventLoadProgress(entry ipfslog.Entry) EventLoadProgress {
	return EventLoadProgress{
		Entry: entry,
	}
}

// EventLoadEnd An event triggered when load ended
type EventLoadEnd struct {
	Logs []ipfslog.Log
}

// NewEventLoadEnd Creates a new EventLoadEnd event
func NewEventLoadEnd(logs []ipfslog.Log) EventLoadEnd {
	return EventLoadEnd{
		Logs: logs,
	}
}
