module berty.tech/go-orbit-db

go 1.12

require (
	berty.tech/go-ipfs-log v0.0.0-20191118100004-2fb04713cace
	github.com/ipfs/go-cid v0.0.2
	github.com/ipfs/go-datastore v0.0.5
	github.com/ipfs/go-ds-leveldb v0.0.2
	github.com/ipfs/go-ipfs v0.4.22
	github.com/ipfs/go-ipfs-files v0.0.3
	github.com/ipfs/go-ipld-cbor v0.0.2
	github.com/ipfs/interface-go-ipfs-core v0.0.8
	github.com/libp2p/go-libp2p v0.0.28
	github.com/libp2p/go-libp2p-core v0.0.6
	github.com/libp2p/go-libp2p-peer v0.2.0 // indirect
	github.com/libp2p/go-libp2p-peerstore v0.0.6
	github.com/pkg/errors v0.8.1
	github.com/polydawn/refmt v0.0.0-20190221155625-df39d6c2d992
	github.com/prometheus/common v0.4.0
	github.com/smartystreets/goconvey v0.0.0-20190222223459-a17d461953aa
	go.uber.org/zap v1.10.0
	google.golang.org/appengine v1.4.0 // indirect
)

replace github.com/dgraph-io/badger => github.com/dgraph-io/badger v0.0.0-20190809121831-9d7b751e85c9

replace github.com/go-critic/go-critic v0.0.0-20181204210945-ee9bf5809ead => github.com/go-critic/go-critic v0.3.5-0.20190210220443-ee9bf5809ead

replace github.com/go-critic/go-critic v0.0.0-20181204210945-c3db6069acc5 => github.com/go-critic/go-critic v0.3.5-0.20190422201921-c3db6069acc5

replace github.com/golangci/errcheck v0.0.0-20181003203344-ef45e06d44b6 => github.com/golangci/errcheck v0.0.0-20181223084120-ef45e06d44b6

replace github.com/golangci/go-tools v0.0.0-20180109140146-af6baa5dc196 => github.com/golangci/go-tools v0.0.0-20190318060251-af6baa5dc196

replace github.com/golangci/gofmt v0.0.0-20181105071733-0b8337e80d98 => github.com/golangci/gofmt v0.0.0-20181222123516-0b8337e80d98

replace github.com/golangci/gosec v0.0.0-20180901114220-66fb7fc33547 => github.com/golangci/gosec v0.0.0-20190211064107-66fb7fc33547

replace github.com/golangci/lint-1 v0.0.0-20180610141402-ee948d087217 => github.com/golangci/lint-1 v0.0.0-20190420132249-ee948d087217

replace mvdan.cc/unparam v0.0.0-20190124213536-fbb59629db34 => mvdan.cc/unparam v0.0.0-20190209190245-fbb59629db34
