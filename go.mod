module github.com/filecoin-project/go-amt-ipld/v3

go 1.16

require (
	github.com/ipfs/go-block-format v0.0.2
	github.com/ipfs/go-cid v0.0.7
	github.com/ipfs/go-ipld-cbor v0.0.4
	github.com/stretchr/testify v1.7.0
	github.com/whyrusleeping/cbor-gen v0.0.0-20200723185710-6a3894a6352b
	golang.org/x/sync v0.3.0
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
)

replace github.com/ipfs/go-ipld-cbor => github.com/vulcanize/go-ipld-cbor v0.0.7-internal
