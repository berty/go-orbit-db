// stores registers constructors for OrbitDB stores
package stores

import orbitdb "github.com/berty/go-orbit-db"

var storeTypes = map[string]orbitdb.StoreConstructor{}

// RegisterStore Registers a new store type which can be used by its name
func RegisterStore(storeType string, constructor orbitdb.StoreConstructor) {
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
func GetConstructor(storeType string) (orbitdb.StoreConstructor, bool) {
	constructor, ok := storeTypes[storeType]

	return constructor, ok
}
