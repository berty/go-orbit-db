package ipfs

import (
	"context"
	"encoding/json"
	"fmt"

	logac "berty.tech/go-ipfs-log/accesscontroller"
	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/io"
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/address"
	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/iface"
	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
	"github.com/pkg/errors"
	"github.com/polydawn/refmt/obj/atlas"
)

type cborWriteAccess struct {
	Write string
}

type ipfsAccessController struct {
	events.EventEmitter
	ipfs        coreapi.CoreAPI
	writeAccess []string
}

func (i *ipfsAccessController) Type() string {
	return "ipfs"
}

func (i *ipfsAccessController) Address() address.Address {
	return nil
}

func (i *ipfsAccessController) CanAppend(entry logac.LogEntry, p identityprovider.Interface, additionalContext accesscontroller.CanAppendAdditionalContext) error {
	key := entry.GetIdentity().ID
	for _, allowedKey := range i.writeAccess {
		if allowedKey == key || allowedKey == "*" {
			return p.VerifyIdentity(entry.GetIdentity())
		}
	}

	return errors.New("not allowed")
}

func (i *ipfsAccessController) GetAuthorizedByRole(role string) ([]string, error) {
	if role == "admin" || role == "write" {
		return i.writeAccess, nil
	}

	return nil, nil
}

func (i *ipfsAccessController) Grant(ctx context.Context, capability string, keyID string) error {
	return errors.New("not implemented - does not exist in JS version")
}

func (i *ipfsAccessController) Revoke(ctx context.Context, capability string, keyID string) error {
	return errors.New("not implemented - does not exist in JS version")
}

func (i *ipfsAccessController) Load(ctx context.Context, address string) error {
	logger().Debug(fmt.Sprintf("reading IPFS access controller write access on hash %s", address))

	c, err := cid.Decode(address)
	if err != nil {
		return errors.Wrap(err, "unable to parse cid")
	}

	res, err := io.ReadCBOR(ctx, i.ipfs, c)
	if err != nil {
		return errors.Wrap(err, "unable to load access controller manifest data")
	}

	manifest := &accesscontroller.Manifest{}
	err = cbornode.DecodeInto(res.RawData(), manifest)
	if err != nil {
		return errors.Wrap(err, "unable to unmarshal access controller manifest data")
	}

	res, err = io.ReadCBOR(ctx, i.ipfs, manifest.Params.GetAddress())
	if err != nil {
		return errors.Wrap(err, "unable to load access controller data")
	}

	writeAccessData := &cborWriteAccess{}
	err = cbornode.DecodeInto(res.RawData(), writeAccessData)
	if err != nil {
		return errors.Wrap(err, "unable to unmarshal access controller data")
	}

	var writeAccess []string
	if err := json.Unmarshal([]byte(writeAccessData.Write), &writeAccess); err != nil {
		return errors.Wrap(err, "unable to unmarshal json write access")
	}

	i.writeAccess = writeAccess

	return nil
}

func (i *ipfsAccessController) Save(ctx context.Context) (accesscontroller.ManifestParams, error) {
	writeAccess, err := json.Marshal(i.writeAccess)
	if err != nil {
		return nil, errors.Wrap(err, "unable to serialize write access")
	}

	c, err := io.WriteCBOR(ctx, i.ipfs, &cborWriteAccess{Write: string(writeAccess)})
	if err != nil {
		return nil, errors.Wrap(err, "unable to save access controller")
	}

	logger().Debug(fmt.Sprintf("saved IPFS access controller write access on hash %s", c.String()))

	return accesscontroller.NewManifestParams(c, false, i.Type()), nil
}

func (i *ipfsAccessController) Close() error {
	return errors.New("not implemented - does not exist in JS version")
}

// NewIPFSAccessController Returns an access controller for IPFS
func NewIPFSAccessController(_ context.Context, db iface.BaseOrbitDB, options accesscontroller.ManifestParams) (accesscontroller.Interface, error) {
	if options == nil {
		return &ipfsAccessController{}, errors.New("an options object must be passed")
	}

	if db == nil {
		return &ipfsAccessController{}, errors.New("an OrbitDB instance is required")
	}

	if len(options.GetAccess("write")) == 0 {
		options.SetAccess("write", []string{db.Identity().ID})
	}

	allowedIDs := options.GetAccess("write")

	return &ipfsAccessController{
		ipfs:        db.IPFS(),
		writeAccess: allowedIDs,
	}, nil
}

var _ accesscontroller.Interface = &ipfsAccessController{}

func init() {
	AtlasEntry := atlas.BuildEntry(cborWriteAccess{}).
		StructMap().
		AddField("Write", atlas.StructMapEntry{SerialName: "write"}).
		Complete()

	cbornode.RegisterCborType(AtlasEntry)
}
