package ipfs

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	logac "berty.tech/go-ipfs-log/accesscontroller"
	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/io"
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/address"
	"berty.tech/go-orbit-db/iface"
	coreapi "github.com/ipfs/boxo/coreiface"
	cid "github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/polydawn/refmt/obj/atlas"
	"go.uber.org/zap"
)

type cborWriteAccess struct {
	Write string
}

type ipfsAccessController struct {
	ipfs          coreapi.CoreAPI
	writeAccess   []string
	muWriteAccess sync.RWMutex
	logger        *zap.Logger
}

func (i *ipfsAccessController) Type() string {
	return "ipfs"
}

func (i *ipfsAccessController) Address() address.Address {
	return nil
}

func (i *ipfsAccessController) CanAppend(entry logac.LogEntry, p identityprovider.Interface, additionalContext accesscontroller.CanAppendAdditionalContext) error {
	i.muWriteAccess.RLock()
	defer i.muWriteAccess.RUnlock()

	key := entry.GetIdentity().ID
	for _, allowedKey := range i.writeAccess {
		if allowedKey == key || allowedKey == "*" {
			return p.VerifyIdentity(entry.GetIdentity())
		}
	}

	return fmt.Errorf("not allowed")
}

func (i *ipfsAccessController) GetAuthorizedByRole(role string) ([]string, error) {
	i.muWriteAccess.RLock()
	defer i.muWriteAccess.RUnlock()

	if role == "admin" || role == "write" {
		return i.writeAccess, nil
	}

	return nil, nil
}

func (i *ipfsAccessController) Grant(ctx context.Context, capability string, keyID string) error {
	return fmt.Errorf("not implemented - does not exist in JS version")
}

func (i *ipfsAccessController) Revoke(ctx context.Context, capability string, keyID string) error {
	return fmt.Errorf("not implemented - does not exist in JS version")
}

func (i *ipfsAccessController) Load(ctx context.Context, address string) error {
	i.logger.Debug(fmt.Sprintf("reading IPFS access controller write access on hash %s", address))

	c, err := cid.Decode(address)
	if err != nil {
		return fmt.Errorf("unable to parse cid: %w", err)
	}

	res, err := io.ReadCBOR(ctx, i.ipfs, c)
	if err != nil {
		return fmt.Errorf("unable to load access controller manifest data: %w", err)
	}

	manifest := &accesscontroller.Manifest{}
	err = cbornode.DecodeInto(res.RawData(), manifest)
	if err != nil {
		return fmt.Errorf("unable to unmarshal access controller manifest data: %w", err)
	}

	res, err = io.ReadCBOR(ctx, i.ipfs, manifest.Params.GetAddress())
	if err != nil {
		return fmt.Errorf("unable to load access controller data: %w", err)
	}

	writeAccessData := &cborWriteAccess{}
	err = cbornode.DecodeInto(res.RawData(), writeAccessData)
	if err != nil {
		return fmt.Errorf("unable to unmarshal access controller data: %w", err)
	}

	var writeAccess []string
	if err := json.Unmarshal([]byte(writeAccessData.Write), &writeAccess); err != nil {
		return fmt.Errorf("unable to unmarshal json write access: %w", err)
	}

	i.muWriteAccess.Lock()
	i.writeAccess = writeAccess
	i.muWriteAccess.Unlock()

	return nil
}

func (i *ipfsAccessController) Save(ctx context.Context) (accesscontroller.ManifestParams, error) {
	i.muWriteAccess.RLock()
	writeAccess, err := json.Marshal(i.writeAccess)
	i.muWriteAccess.RUnlock()

	if err != nil {
		return nil, fmt.Errorf("unable to serialize write access: %w", err)
	}

	c, err := io.WriteCBOR(ctx, i.ipfs, &cborWriteAccess{Write: string(writeAccess)}, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to save access controller: %w", err)
	}

	i.logger.Debug(fmt.Sprintf("saved IPFS access controller write access on hash %s", c.String()))

	return accesscontroller.NewManifestParams(c, false, i.Type()), nil
}

func (i *ipfsAccessController) Close() error {
	return fmt.Errorf("not implemented - does not exist in JS version")
}

// NewIPFSAccessController Returns an access controller for IPFS
func NewIPFSAccessController(_ context.Context, db iface.BaseOrbitDB, params accesscontroller.ManifestParams, options ...accesscontroller.Option) (accesscontroller.Interface, error) {
	if params == nil {
		return &ipfsAccessController{}, fmt.Errorf("an options object must be passed")
	}

	if db == nil {
		return &ipfsAccessController{}, fmt.Errorf("an OrbitDB instance is required")
	}

	if len(params.GetAccess("write")) == 0 {
		params.SetAccess("write", []string{db.Identity().ID})
	}

	allowedIDs := params.GetAccess("write")

	ac := &ipfsAccessController{
		ipfs:        db.IPFS(),
		writeAccess: allowedIDs,
	}

	for _, o := range options {
		o(ac)
	}

	return ac, nil
}

func (i *ipfsAccessController) SetLogger(logger *zap.Logger) {
	i.muWriteAccess.Lock()
	defer i.muWriteAccess.Unlock()

	i.logger = logger
}

func (i *ipfsAccessController) Logger() *zap.Logger {
	i.muWriteAccess.RLock()
	defer i.muWriteAccess.RUnlock()

	return i.logger
}

var _ accesscontroller.Interface = &ipfsAccessController{}

func init() {
	AtlasEntry := atlas.BuildEntry(cborWriteAccess{}).
		StructMap().
		AddField("Write", atlas.StructMapEntry{SerialName: "write"}).
		Complete()

	cbornode.RegisterCborType(AtlasEntry)
}
