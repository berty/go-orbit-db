package ipfs

import (
	coreapi "github.com/ipfs/interface-go-ipfs-core"
)

type Services interface {
	Dag() coreapi.APIDagService
	PubSub() coreapi.PubSubAPI
	Key() coreapi.KeyAPI
	Object() coreapi.ObjectAPI
}
