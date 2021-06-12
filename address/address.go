package address

import (
	"fmt"
	"path"
	"strings"

	cid "github.com/ipfs/go-cid"
	"github.com/pkg/errors"
)

// Address A store address
type Address interface {
	// GetRoot Returns the root CID for the store
	GetRoot() cid.Cid

	// GetPath Returns the path for the store
	GetPath() string

	// String Returns the whole address for the store as a string
	String() string
}

type address struct {
	root cid.Cid
	path string
}

func (a *address) String() string {
	return path.Join("/orbitdb", a.root.String(), a.path)
}

func (a *address) GetRoot() cid.Cid {
	return a.root
}

func (a *address) GetPath() string {
	return a.path
}

// IsValid Checks if a given name is a valid address
func IsValid(name string) error {
	name = strings.TrimPrefix(name, "/orbitdb/")
	parts := strings.Split(name, "/")

	var accessControllerHash cid.Cid

	accessControllerHash, err := cid.Decode(parts[0])
	if err != nil {
		return errors.Wrap(err, "address is invalid")
	}

	if accessControllerHash.String() == "" {
		return errors.New("address is invalid")
	}

	return nil
}

// Parse Returns an Address instance if the given path is valid
func Parse(path string) (Address, error) {
	if err := IsValid(path); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("not a valid OrbitDB address: %s", path))
	}

	path = strings.TrimPrefix(path, "/orbitdb/")
	parts := strings.Split(path, "/")

	c, err := cid.Decode(parts[0])
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse CID")
	}

	return &address{
		root: c,
		path: strings.Join(parts[1:], "/"),
	}, nil
}
