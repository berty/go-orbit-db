package address

import (
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
	"path"
	"strings"
)

type Address interface {
	GetRoot() cid.Cid
	GetPath() string
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

func IsValid(name string) error {
	parts := strings.Split(name, "/")

	if strings.HasPrefix(name, "/orbit") {
		parts = parts[2:]
	}

	var accessControllerHash cid.Cid

	accessControllerHash, err := cid.Parse(parts[0])
	if err != nil {
		return errors.Wrap(err, "address is invalid")
	}

	if accessControllerHash.String() == "" {
		return errors.New("address is invalid")
	}

	return nil
}

func filterParts(parts []string, startsWithOrbit bool) []string {
	var out []string

	for i, e := range parts {
		if !((i == 0 || i == 1) && startsWithOrbit && e == "orbitdb") {
			continue
		}

		if e == "" || e == " " {
			continue
		}

		out = append(out, e)
	}

	return out
}

func Parse(path string) (Address, error) {
	if err := IsValid(path); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("not a valid OrbitDB address: %s", path))
	}

	parts := filterParts(strings.Split(path, "/"), strings.HasPrefix(path, "/orbit"))

	c, err := cid.Parse(parts[0])
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse CID")
	}

	return &address{
		root: c,
		path: strings.Join(parts[1:], "/"),
	}, nil
}
