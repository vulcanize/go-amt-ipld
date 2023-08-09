package amt

import (
	"io"
	"sync/atomic"

	cbg "github.com/whyrusleeping/cbor-gen"
)

var _ cbg.CBORUnmarshaler = &CBORSinkCounter{}

type CBORSinkCounter struct {
	calledTimes uint64
}

func (c *CBORSinkCounter) UnmarshalCBOR(reader io.Reader) error {
	atomic.AddUint64(&c.calledTimes, 1)
	return nil
}

func uint64Pow(n, m uint64) uint64 {
	if m == 0 {
		return 1
	}
	result := n
	for i := uint64(2); i <= m; i++ {
		result *= n
	}
	return result
}
