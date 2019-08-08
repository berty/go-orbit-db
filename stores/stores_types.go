// stores registers constructors for OrbitDB stores
package stores

import (
	"github.com/berty/go-orbit-db/iface"
)

var storeTypes = map[string]iface.StoreConstructor{}

// RegisterStore Registers a new store type which can be used by its name
func RegisterStore(storeType string, constructor iface.StoreConstructor) {
	storeTypes[storeType] = constructor
}

// UnregisterStore Unregisters a store type by its name
func UnregisterStore(storeType string) {
	delete(storeTypes, storeType)
}

// StoreTypesNames Lists available stores names
func StoreTypesNames() []string {
	var names []string

	for k := range storeTypes {
		names = append(names, k)
	}

	return names
}

// GetConstructor Gets a store constructor
func GetConstructor(storeType string) (iface.StoreConstructor, bool) {
	constructor, ok := storeTypes[storeType]

	return constructor, ok
}
