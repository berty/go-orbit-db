package operation

import (
	"encoding/json"

	ipfslog "berty.tech/go-ipfs-log"

	"github.com/pkg/errors"
)

type operation struct {
	Key   *string       `json:"key,omitempty"`
	Op    string        `json:"op,omitempty"`
	Value []byte        `json:"value,omitempty"`
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

// ParseOperation Gets the operation from an entry
func ParseOperation(e ipfslog.Entry) (Operation, error) {
	if e == nil {
		return nil, errors.New("an entry must be provided")
	}

	op := operation{}

	err := json.Unmarshal(e.GetPayload(), &op)
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
