package utils

import (
	"context"
	"fmt"

	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/iface"
	cid "github.com/ipfs/go-cid"
)

// Create Creates a new access controller and returns the manifest CID
func Create(ctx context.Context, db iface.BaseOrbitDB, controllerType string, params accesscontroller.ManifestParams, options ...accesscontroller.Option) (cid.Cid, error) {
	AccessController, ok := db.GetAccessControllerType(controllerType)
	if !ok {
		return cid.Cid{}, fmt.Errorf("unrecognized access controller on create")
	}

	if params.GetSkipManifest() {
		return params.GetAddress(), nil
	}

	ac, err := AccessController(ctx, db, params, options...)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("unable to init access controller: %w", err)
	}

	acParams, err := ac.Save(ctx)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("unable to save access controller: %w", err)
	}

	return accesscontroller.CreateManifest(ctx, db.IPFS(), controllerType, acParams)
}

// Resolve Resolves an access controller using its manifest address
func Resolve(ctx context.Context, db iface.BaseOrbitDB, manifestAddress string, params accesscontroller.ManifestParams, options ...accesscontroller.Option) (accesscontroller.Interface, error) {
	manifest, err := accesscontroller.ResolveManifest(ctx, db.IPFS(), manifestAddress, params)
	if err != nil {
		return nil, fmt.Errorf("unable to resolve manifest: %w", err)
	}

	accessControllerConstructor, ok := db.GetAccessControllerType(manifest.Type)
	if !ok {
		return nil, fmt.Errorf("unrecognized access controller on resolve")
	}

	// TODO: options
	accessController, err := accessControllerConstructor(ctx, db, manifest.Params, options...)
	if err != nil {
		return nil, fmt.Errorf("unable to create access controller: %w", err)
	}

	err = accessController.Load(ctx, params.GetAddress().String())
	if err != nil {
		return nil, fmt.Errorf("unable to load access controller: %w", err)
	}

	return accessController, nil
}
