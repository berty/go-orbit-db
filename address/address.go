package address

import (
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
	"go.uber.org/zap"
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
	name = strings.TrimPrefix(name, "/orbitdb/")
	parts := strings.Split(name, "/")

	logger().Debug(fmt.Sprintf("Address.IsValid: Tested name: %s (from %s)", parts[0], name))

	var accessControllerHash cid.Cid

	accessControllerHash, err := cid.Decode(parts[0])
	if err != nil {
		logger().Debug("address is invalid", zap.Error(err))
		return errors.Wrap(err, "address is invalid")
	}

	if accessControllerHash.String() == "" {
		logger().Debug("accessControllerHash is empty")
		return errors.New("address is invalid")
	}

	logger().Debug("Address.IsValid: seems ok, returning nil")

	return nil
}

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
