package utils

import (
	"context"

	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/iface"
	cid "github.com/ipfs/go-cid"
	"github.com/pkg/errors"
)

// Create Creates a new access controller and returns the manifest CID
func Create(ctx context.Context, db iface.BaseOrbitDB, controllerType string, params accesscontroller.ManifestParams, options ...accesscontroller.Option) (cid.Cid, error) {
	AccessController, ok := db.GetAccessControllerType(controllerType)
	if !ok {
		return cid.Cid{}, errors.New("unrecognized access controller on create")
	}

	if params.GetSkipManifest() {
		return params.GetAddress(), nil
	}

	ac, err := AccessController(ctx, db, params, options...)
	if err != nil {
		return cid.Cid{}, errors.Wrap(err, "unable to init access controller")
	}

	acParams, err := ac.Save(ctx)
	if err != nil {
		return cid.Cid{}, errors.Wrap(err, "unable to save access controller")
	}

	return accesscontroller.CreateManifest(ctx, db.IPFS(), controllerType, acParams)
}

// Resolve Resolves an access controller using its manifest address
func Resolve(ctx context.Context, db iface.BaseOrbitDB, manifestAddress string, params accesscontroller.ManifestParams, options ...accesscontroller.Option) (accesscontroller.Interface, error) {
	manifest, err := accesscontroller.ResolveManifest(ctx, db.IPFS(), manifestAddress, params)
	if err != nil {
		return nil, errors.Wrap(err, "unable to resolve manifest")
	}

	accessControllerConstructor, ok := db.GetAccessControllerType(manifest.Type)
	if !ok {
		return nil, errors.New("unrecognized access controller on resolve")
	}

	// TODO: options
	accessController, err := accessControllerConstructor(ctx, db, manifest.Params, options...)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create access controller")
	}

	err = accessController.Load(ctx, params.GetAddress().String())
	if err != nil {
		return nil, errors.Wrap(err, "unable to load access controller")
	}

	return accessController, nil
}
