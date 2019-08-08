// base is a set of functions common to all access controllers
package base

import (
	"context"
	"github.com/berty/go-orbit-db/accesscontroller"
	"github.com/berty/go-orbit-db/iface"
	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
)

// CreateAccessControllerOptions Options used to create an Access Controller
type CreateAccessControllerOptions struct {
	Type         string
	SkipManifest bool
	Name         string
	Access       map[string][]string
	Address      string
}

// Required prototype for custom controllers constructors
type ControllerConstructor func(context.Context, iface.OrbitDB, *CreateAccessControllerOptions) (accesscontroller.Interface, error)

var supportedTypes = map[string]ControllerConstructor{}

// Create Creates a new access controller and returns the manifest CID
func Create(ctx context.Context, db iface.OrbitDB, controllerType string, options *CreateAccessControllerOptions) (cid.Cid, error) {
	AccessController, ok := supportedTypes[controllerType]
	if !ok {
		return cid.Cid{}, errors.New("unrecognized access controller on create")
	}

	ac, err := AccessController(ctx, db, options)
	if err != nil {
		return cid.Cid{}, errors.Wrap(err, "unable to init access controller")
	}

	params, err := ac.Save(ctx)
	if err != nil {
		return cid.Cid{}, errors.Wrap(err, "unable to save access controller")
	}

	return accesscontroller.CreateManifest(ctx, db.IPFS(), controllerType, params)
}

// Resolve Resolves an access controller using its manifest address
func Resolve(ctx context.Context, db iface.OrbitDB, manifestAddress string, params accesscontroller.ManifestParams) (accesscontroller.Interface, error) {
	manifest, err := accesscontroller.ResolveManifest(ctx, db.IPFS(), manifestAddress, params)
	if err != nil {
		return nil, errors.Wrap(err, "unable to resolve manifest")
	}

	accessControllerConstructor, ok := supportedTypes[manifest.Type]
	if !ok {
		return nil, errors.New("unrecognized access controller on resolve")
	}

	// TODO: options
	accessController, err := accessControllerConstructor(ctx, db, &CreateAccessControllerOptions{
		SkipManifest: manifest.Params.SkipManifest,
		Address:      manifest.Params.Address.String(),
		Type:         manifest.Params.Type,
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to create access controller")
	}

	err = accessController.Load(ctx, params.GetAddress().String())
	if err != nil {
		return nil, errors.Wrap(err, "unable to load access controller")
	}

	return accessController, nil
}

// IsSupported Checks whether an access controller type is supported
func IsSupported(controllerType string) bool {
	_, ok := supportedTypes[controllerType]

	return ok
}

// AddAccessController Registers an access controller type using its constructor
func AddAccessController(constructor ControllerConstructor) error {
	if constructor == nil {
		return errors.New("accessController class needs to be given as an option")
	}

	controller, _ := constructor(context.Background(), nil, &CreateAccessControllerOptions{})

	controllerType := controller.Type()

	if controller.Type() == "" {
		panic("controller type cannot be empty")
	}

	supportedTypes[controllerType] = constructor

	return nil
}

// RemoveAccessController Unregister an access controller type
func RemoveAccessController(controllerType string) {
	delete(supportedTypes, controllerType)
}
