package operation

import (
	"encoding/json"

	"berty.tech/go-ipfs-log/entry"
	"github.com/pkg/errors"
)

type operation struct {
	Key   *string      `json:"key,omitempty"`
	Op    string       `json:"op,omitempty"`
	Value []byte       `json:"value,omitempty"`
	Entry *entry.Entry `json:"-"`
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

func (o *operation) GetEntry() *entry.Entry {
	//return nil
	return o.Entry
}

// ParseOperation Gets the operation from an entry
func ParseOperation(e *entry.Entry) (Operation, error) {
	op := operation{}

	err := json.Unmarshal(e.Payload, &op)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse operation json")
	}

	op.Entry = e

	return &op, nil
}

// NewOperation Creates a new operation
func NewOperation(key *string, op string, value []byte) Operation {
	return &operation{
		Key:   key,
		Op:    op,
		Value: value,
	}
}

var _ Operation = &operation{}
