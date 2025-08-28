package operation

import (
	"encoding/json"
	"fmt"

	ipfslog "github.com/stateless-minds/go-ipfs-log"
)

type opDoc struct {
	Key   string `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`
}

func (b *opDoc) GetKey() string {
	return b.Key
}

func (b *opDoc) GetValue() []byte {
	return b.Value
}

type operation struct {
	Key   *string       `json:"key,omitempty"`
	Op    string        `json:"op,omitempty"`
	Value []byte        `json:"value,omitempty"`
	Docs  []*opDoc      `json:"docs,omitempty"`
	Entry ipfslog.Entry `json:"-"`
}

func (o *operation) Marshal() ([]byte, error) {
	return json.Marshal(o)
}

func (o *operation) GetKey() *string {
	return o.Key
}

func (o *operation) GetOperation() string {
	return o.Op
}

func (o *operation) GetValue() []byte {
	return o.Value
}

func (o *operation) GetEntry() ipfslog.Entry {
	return o.Entry
}

func (o *operation) GetDocs() []OpDoc {
	ret := make([]OpDoc, len(o.Docs))

	for i, val := range o.Docs {
		ret[i] = val
	}

	return ret
}

// ParseOperation Gets the operation from an entry
func ParseOperation(e ipfslog.Entry) (Operation, error) {
	if e == nil {
		return nil, fmt.Errorf("an entry must be provided")
	}

	op := operation{}

	err := json.Unmarshal(e.GetPayload(), &op)
	if err != nil {
		return nil, fmt.Errorf("unable to parse operation json: %w", err)
	}

	op.Entry = e

	return &op, nil
}

func NewOpDoc(key string, value []byte) OpDoc {
	return &opDoc{
		Key:   key,
		Value: value,
	}
}

// NewOperation Creates a new operation
func NewOperation(key *string, op string, value []byte) Operation {
	return &operation{
		Key:   key,
		Op:    op,
		Value: value,
	}
}

// NewOperationWithDocuments Creates a new operation from a map of batched documents
func NewOperationWithDocuments(key *string, op string, docs map[string][]byte) Operation {
	_docs := make([]*opDoc, len(docs))

	i := 0
	for k, v := range docs {
		_docs[i] = &opDoc{
			Key:   k,
			Value: v,
		}
		i++
	}

	return &operation{
		Key:  key,
		Op:   op,
		Docs: _docs,
	}
}

var _ Operation = &operation{}
