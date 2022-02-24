module berty.tech/go-orbit-db

go 1.16

require (
	berty.tech/go-ipfs-log v1.6.0
	github.com/golang/snappy v0.0.1 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20190812055157-5d271430af9f // indirect
	github.com/ipfs/go-cid v0.1.0
	github.com/ipfs/go-datastore v0.5.1
	github.com/ipfs/go-ds-leveldb v0.5.0
	github.com/ipfs/go-ipfs v0.11.0
	github.com/ipfs/go-ipfs-config v0.18.0
	github.com/ipfs/go-ipfs-files v0.0.9
	github.com/ipfs/go-ipld-cbor v0.0.6
	github.com/ipfs/interface-go-ipfs-core v0.5.2
	github.com/libp2p/go-eventbus v0.2.1
	github.com/libp2p/go-libp2p v0.17.0
	github.com/libp2p/go-libp2p-core v0.13.0
	github.com/libp2p/go-libp2p-pubsub v0.6.0
	github.com/pkg/errors v0.9.1
	github.com/polydawn/refmt v0.0.0-20201211092308-30ac6d18308e
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/otel v0.20.0
	go.opentelemetry.io/otel/trace v0.20.0
	go.uber.org/goleak v1.1.11
	go.uber.org/zap v1.19.1
	golang.org/x/lint v0.0.0-20201208152925-83fdc39ff7b5 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
)

replace berty.tech/go-ipfs-log => github.com/gfanton/go-ipfs-log v0.0.0-20220222191845-0aaf600b5311
