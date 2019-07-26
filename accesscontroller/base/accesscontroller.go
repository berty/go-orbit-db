package base

import (
	"context"
	orbitdb "github.com/berty/go-orbit-db"
	"github.com/berty/go-orbit-db/accesscontroller"
	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
)

type CreateAccessControllerOptions struct {
	Type         string
	SkipManifest bool
	Name         string
	Access       map[string][]string
	WriteAccess  []string
	AdminAccess  []string
	Address      string
}

type ControllerConstructor func(context.Context, orbitdb.OrbitDB, *CreateAccessControllerOptions) (accesscontroller.Interface, error)

var supportedTypes = map[string]ControllerConstructor{}

func Create(ctx context.Context, db orbitdb.OrbitDB, controllerType string, options *CreateAccessControllerOptions) (cid.Cid, error) {
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

func Resolve(ctx context.Context, db orbitdb.OrbitDB, manifestAddress string, params accesscontroller.ManifestParams) (accesscontroller.Interface, error) {
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

func IsSupported(controllerType string) bool {
	_, ok := supportedTypes[controllerType]

	return ok
}

type AddAccessControllerOptions struct {
	ControllerConstructor ControllerConstructor
}

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

func RemoveAccessController(controllerType string) {
	delete(supportedTypes, controllerType)
}
