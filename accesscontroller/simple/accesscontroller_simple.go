package simple

import (
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/identityprovider"
	"bytes"
	"github.com/berty/go-orbit-db/accesscontroller"
	"github.com/pkg/errors"
)

type simpleAccessController struct {
	identity *identityprovider.Identity
}

func (o *simpleAccessController) CanAppend(e *entry.Entry, p identityprovider.Interface) error {
	if bytes.Compare(e.Identity.PublicKey, o.identity.PublicKey) != 0 {
		return errors.New("identity doesn't match")
	}

	return nil
}

func NewSimpleAccessController(identity *identityprovider.Identity) (accesscontroller.SimpleInterface, error) {
	return &simpleAccessController{
		identity: identity,
	}, nil
}

var _ accesscontroller.SimpleInterface = &simpleAccessController{}
