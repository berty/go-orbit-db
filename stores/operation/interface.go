package operation

import "berty.tech/go-ipfs-log/entry"

type Operation interface {
	GetKey() *string
	GetOperation() string
	GetValue() []byte
	GetEntry() *entry.Entry
	Marshal() ([]byte, error)
}
