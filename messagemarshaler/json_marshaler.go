package messagemarshaler

import (
	"encoding/json"

	"github.com/stateless-minds/go-orbit-db/iface"
)

type JSONMarshaler struct{}

func (JSONMarshaler) Marshal(m *iface.MessageExchangeHeads) ([]byte, error) {
	return json.Marshal(m)
}

func (JSONMarshaler) Unmarshal(data []byte, m *iface.MessageExchangeHeads) error {
	return json.Unmarshal(data, m)
}
