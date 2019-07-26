package stores

import orbitdb "github.com/berty/go-orbit-db"

var storeTypes = map[string]orbitdb.StoreConstructor{}

func RegisterStore(storeType string, constructor orbitdb.StoreConstructor) {
	storeTypes[storeType] = constructor
}

func UnregisterStore(storeType string) {
	delete(storeTypes, storeType)
}

func StoreTypesNames() []string {
	var names []string

	for k := range storeTypes {
		names = append(names, k)
	}

	return names
}

func GetConstructor(storeType string) (orbitdb.StoreConstructor, bool) {
	constructor, ok := storeTypes[storeType]

	return constructor, ok
}
