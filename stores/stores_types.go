package stores

var storeTypes = map[string]Constructor{}

func RegisterStore(storeType string, constructor Constructor) {
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

func GetConstructor(storeType string) (Constructor, bool) {
	constructor, ok := storeTypes[storeType]

	return constructor, ok
}
