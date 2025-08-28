//go:build js

package cacheleveldown

import (
	"berty.tech/go-orbit-db/cache"
)

var InMemoryDirectory = ":memory:"

// New Creates a new leveldb data store
func New(opts *cache.Options) cache.Interface {
	panic("cacheleveldown not implemented in js")
}
