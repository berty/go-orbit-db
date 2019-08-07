package accesscontroller

import (
	"berty.tech/go-ipfs-log/io"
	"context"
	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
	"github.com/pkg/errors"
	"github.com/polydawn/refmt/obj/atlas"
	"strings"
)

// Manifest An access controller manifest
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

// Create a new manifest parameters instance
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

// ManifestParams List of getters for a manifest parameters
type ManifestParams interface {
	GetSkipManifest() bool
	GetAddress() cid.Cid
	GetType() string
}

// CreateManifest Creates a new manifest and returns its CID
func CreateManifest(ctx context.Context, ipfs coreapi.CoreAPI, controllerType string, params ManifestParams) (cid.Cid, error) {
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

// ResolveManifest Retrieves a manifest from its address
func ResolveManifest(ctx context.Context, ipfs coreapi.CoreAPI, manifestAddress string, params ManifestParams) (*Manifest, error) {
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

	if strings.HasPrefix(manifestAddress, "/ipfs") {
		manifestAddress = strings.Split(manifestAddress, "/")[2]
	}

	c, err := cid.Decode(manifestAddress)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse CID")
	}

	node, err := io.ReadCBOR(ctx, ipfs, c)
	if err != nil {
		return nil, errors.Wrap(err, "unable to fetch manifest data")
	}

	manifest := &Manifest{}
	err = cbornode.DecodeInto(node.RawData(), &manifest)
	if err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal")
	}

	return manifest, nil
}

var _ ManifestParams = &manifestParams{}

func init() {
	atlasManifest := atlas.BuildEntry(Manifest{}).
		StructMap().
		AddField("Type", atlas.StructMapEntry{SerialName: "type"}).
		AddField("Params", atlas.StructMapEntry{SerialName: "manifest"}).
		Complete()

	atlasManifestParams := atlas.BuildEntry(manifestParams{}).
		StructMap().
		AddField("SkipManifest", atlas.StructMapEntry{SerialName: "skip_manifest"}).
		AddField("Address", atlas.StructMapEntry{SerialName: "address"}).
		AddField("Type", atlas.StructMapEntry{SerialName: "type"}).
		Complete()

	cbornode.RegisterCborType(atlasManifest)
	cbornode.RegisterCborType(atlasManifestParams)
}
