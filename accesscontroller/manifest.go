package accesscontroller

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"berty.tech/go-ipfs-log/io"
	coreapi "github.com/ipfs/boxo/coreiface"
	cid "github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/polydawn/refmt/obj/atlas"
	"go.uber.org/zap"
)

// Manifest An access controller manifest
type Manifest struct {
	Type   string
	Params *CreateAccessControllerOptions
}

// CreateAccessControllerOptions Options used to create an Access Controller
type CreateAccessControllerOptions struct {
	SkipManifest bool
	Address      cid.Cid
	Type         string
	Name         string
	Access       map[string][]string

	muAccess sync.RWMutex
}

func CloneManifestParams(m ManifestParams) *CreateAccessControllerOptions {
	return &CreateAccessControllerOptions{
		Type:         m.GetType(),
		SkipManifest: m.GetSkipManifest(),
		Name:         m.GetName(),
		Access:       m.GetAllAccess(),
		Address:      m.GetAddress(),
	}
}

func (m *CreateAccessControllerOptions) GetName() string {
	return m.Name
}

func (m *CreateAccessControllerOptions) SetName(name string) {
	m.Name = name
}

func (m *CreateAccessControllerOptions) SetAccess(role string, allowed []string) {
	m.muAccess.Lock()
	defer m.muAccess.Unlock()

	if m.Access == nil {
		m.Access = make(map[string][]string)
	}

	m.Access[role] = allowed
}

func (m *CreateAccessControllerOptions) GetAccess(role string) []string {
	m.muAccess.RLock()
	defer m.muAccess.RUnlock()

	return m.Access[role]
}

func (m *CreateAccessControllerOptions) GetAllAccess() map[string][]string {
	m.muAccess.RLock()
	defer m.muAccess.RUnlock()

	if m.Access == nil {
		return map[string][]string{}
	}

	accessCopy := map[string][]string{}
	for k, v := range m.Access {
		accessCopy[k] = v
	}

	return accessCopy
}

func (m *CreateAccessControllerOptions) GetType() string {
	return m.Type
}

func (m *CreateAccessControllerOptions) SetType(t string) {
	m.Type = t
}

// Create a new manifest parameters instance
func NewManifestParams(address cid.Cid, skipManifest bool, manifestType string) ManifestParams {
	return &CreateAccessControllerOptions{
		Address:      address,
		SkipManifest: skipManifest,
		Type:         manifestType,
	}
}

func NewEmptyManifestParams() ManifestParams {
	return &CreateAccessControllerOptions{}
}

func NewSimpleManifestParams(manifestType string, access map[string][]string) ManifestParams {
	return &CreateAccessControllerOptions{
		SkipManifest: true,
		Access:       access,
		Type:         manifestType,
	}
}

func (m *CreateAccessControllerOptions) GetSkipManifest() bool {
	return m.SkipManifest
}

func (m *CreateAccessControllerOptions) GetAddress() cid.Cid {
	return m.Address
}

func (m *CreateAccessControllerOptions) SetAddress(c cid.Cid) {
	m.Address = c
}

// ManifestParams List of getters for a manifest parameters
type ManifestParams interface {
	GetSkipManifest() bool
	GetAddress() cid.Cid
	SetAddress(cid.Cid)
	GetType() string
	SetType(string)

	GetName() string
	SetName(string)
	SetAccess(string, []string)
	GetAccess(string) []string
	GetAllAccess() map[string][]string
}

// CreateManifest Creates a new manifest and returns its CID
func CreateManifest(ctx context.Context, ipfs coreapi.CoreAPI, controllerType string, params ManifestParams) (cid.Cid, error) {
	if params.GetSkipManifest() {
		return params.GetAddress(), nil
	}

	manifest := &Manifest{
		Type: controllerType,
		Params: &CreateAccessControllerOptions{
			Address:      params.GetAddress(),
			SkipManifest: params.GetSkipManifest(),
		},
	}

	return io.WriteCBOR(ctx, ipfs, manifest, nil)
}

// ResolveManifest Retrieves a manifest from its address
func ResolveManifest(ctx context.Context, ipfs coreapi.CoreAPI, manifestAddress string, params ManifestParams) (*Manifest, error) {
	if params.GetSkipManifest() {
		if params.GetType() == "" {
			return nil, fmt.Errorf("no manifest, access-controller type required")
		}

		manifest := &Manifest{
			Type:   params.GetType(),
			Params: CloneManifestParams(params),
		}

		return manifest, nil
	}

	if strings.HasPrefix(manifestAddress, "/ipfs") {
		manifestAddress = strings.Split(manifestAddress, "/")[2]
	}

	c, err := cid.Decode(manifestAddress)
	if err != nil {
		return nil, fmt.Errorf("unable to parse CID: %w", err)
	}

	node, err := io.ReadCBOR(ctx, ipfs, c)
	if err != nil {
		return nil, fmt.Errorf("unable to fetch manifest data: %w", err)
	}

	manifest := &Manifest{}
	err = cbornode.DecodeInto(node.RawData(), &manifest)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal: %w", err)
	}

	return manifest, nil
}

var _ ManifestParams = &CreateAccessControllerOptions{}

func WithLogger(logger *zap.Logger) func(ac Interface) {
	return func(ac Interface) {
		ac.SetLogger(logger)
	}
}

func init() {
	atlasManifest := atlas.BuildEntry(Manifest{}).
		StructMap().
		AddField("Type", atlas.StructMapEntry{SerialName: "type"}).
		AddField("Params", atlas.StructMapEntry{SerialName: "manifest"}).
		Complete()

	atlasManifestParams := atlas.BuildEntry(CreateAccessControllerOptions{}).
		StructMap().
		AddField("SkipManifest", atlas.StructMapEntry{SerialName: "skip_manifest"}).
		AddField("Address", atlas.StructMapEntry{SerialName: "address"}).
		AddField("Type", atlas.StructMapEntry{SerialName: "type"}).
		Complete()

	cbornode.RegisterCborType(atlasManifest)
	cbornode.RegisterCborType(atlasManifestParams)
}
