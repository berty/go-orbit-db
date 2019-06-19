package operation

import (
	"berty.tech/go-ipfs-log/entry"
	"encoding/json"
	"github.com/pkg/errors"
)

type operation struct {
	key   string
	op    string
	value []byte
}

func (o *operation) Marshal() ([]byte, error) {
	return json.Marshal(o)
}

func (o *operation) GetKey() string {
	return o.key
}

func (o *operation) GetOperation() string {
	return o.op
}

func (o *operation) GetValue() []byte {
	return o.value
}

func ParseOperation(e *entry.Entry) (Operation, error) {
	op := operation{}

	err := json.Unmarshal(e.Payload, &op)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse operation json")
	}

	return &op, nil
}

func NewOperation(key, op string, value []byte) Operation {
	return &operation{
		key:   key,
		op:    op,
		value: value,
	}
}

var _ Operation = &operation{}
