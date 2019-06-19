package accesscontroller

import (
	"berty.tech/go-ipfs-log/io"
	"context"
	"github.com/berty/go-orbit-db/ipfs"
	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/pkg/errors"
	"strings"
)

type Manifest struct {
	Type   string
	Params *manifestParams
}

type manifestParams struct {
	SkipManifest bool
	Address      cid.Cid
	Type         string
}

func (m *manifestParams) GetType() string {
	return m.Type
}

func NewManifestParams(address cid.Cid, skipManifest bool, manifestType string) ManifestParams {
	return &manifestParams{
		Address:      address,
		SkipManifest: skipManifest,
		Type:         manifestType,
	}
}

func (m *manifestParams) GetSkipManifest() bool {
	return m.SkipManifest
}

func (m *manifestParams) GetAddress() cid.Cid {
	return m.Address
}

type ManifestParams interface {
	GetSkipManifest() bool
	GetAddress() cid.Cid
	GetType() string
}

func CreateManifest(ctx context.Context, ipfs ipfs.Services, controllerType string, params ManifestParams) (cid.Cid, error) {
	if params.GetSkipManifest() {
		return params.GetAddress(), nil
	}

	manifest := &Manifest{
		Type: controllerType,
		Params: &manifestParams{
			Address:      params.GetAddress(),
			SkipManifest: params.GetSkipManifest(),
		},
	}

	return io.WriteCBOR(ctx, ipfs, manifest)
}

func ResolveManifest(ctx context.Context, services ipfs.Services, manifestAddress string, params ManifestParams) (*Manifest, error) {
	if params.GetSkipManifest() {
		if params.GetType() == "" {
			return nil, errors.New("no manifest, access-controller type required")
		}

		return &Manifest{
			Type: params.GetType(),
			Params: &manifestParams{
				Address: params.GetAddress(),
			},
		}, nil

	}

	// TODO: ensure this is a valid multihash
	if strings.HasPrefix(manifestAddress, "/ipfs") {
		manifestAddress = strings.Split(manifestAddress, "/")[2]
	}

	c, err := cid.Parse(manifestAddress)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse CID")
	}

	node, err := io.ReadCBOR(ctx, services, c)
	if err != nil {
		return nil, errors.Wrap(err, "unable to fetch manifest data")
	}

	manifest := &Manifest{}
	err = cbornode.DecodeInto(node.RawData(), &manifestAddress)
	if err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal")
	}

	return manifest, nil
}

var _ ManifestParams = &manifestParams{}
