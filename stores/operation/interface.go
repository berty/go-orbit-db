package operation

import (
	ipfslog "github.com/stateless-minds/go-ipfs-log"
)

type OpDoc interface {
	GetKey() string
	GetValue() []byte
}

// Operation Describe an CRDT operation
type Operation interface {
	// GetKey Gets a key if applicable (ie. key value stores)
	GetKey() *string

	// GetOperation Returns an operation name (ie. append, put, remove)
	GetOperation() string

	// GetValue Returns the operation payload
	GetValue() []byte

	// GetEntry Gets the underlying IPFS log Entry
	GetEntry() ipfslog.Entry

	// GetDocs Gets the list of documents
	GetDocs() []OpDoc

	// Marshal Serializes the operation
	Marshal() ([]byte, error)
}
