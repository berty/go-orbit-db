package utils

import (
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/iface"
	"context"
	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
)

// Create Creates a new access controller and returns the manifest CID
func Create(ctx context.Context, db iface.BaseOrbitDB, controllerType string, options accesscontroller.ManifestParams) (cid.Cid, error) {
	AccessController, ok := db.GetAccessControllerType(controllerType)
	if !ok {
		return cid.Cid{}, errors.New("unrecognized access controller on create")
	}

	if options.GetSkipManifest() {
		return options.GetAddress(), nil
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
func Resolve(ctx context.Context, db iface.BaseOrbitDB, manifestAddress string, params accesscontroller.ManifestParams) (accesscontroller.Interface, error) {
	manifest, err := accesscontroller.ResolveManifest(ctx, db.IPFS(), manifestAddress, params)
	if err != nil {
		return nil, errors.Wrap(err, "unable to resolve manifest")
	}

	accessControllerConstructor, ok := db.GetAccessControllerType(manifest.Type)
	if !ok {
		return nil, errors.New("unrecognized access controller on resolve")
	}

	// TODO: options
	accessController, err := accessControllerConstructor(ctx, db, manifest.Params)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create access controller")
	}

	err = accessController.Load(ctx, params.GetAddress().String())
	if err != nil {
		return nil, errors.Wrap(err, "unable to load access controller")
	}

	return accessController, nil
}
