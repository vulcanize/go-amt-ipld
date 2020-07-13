// Code generated by github.com/whyrusleeping/cbor-gen. DO NOT EDIT.

package amt

import (
	"fmt"
	"io"

	cid "github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"
)

var _ = xerrors.Errorf

var lengthBufRoot = []byte{131}

func (t *Root) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write(lengthBufRoot); err != nil {
		return err
	}

	scratch := make([]byte, 9)

	// t.Height (uint64) (uint64)

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajUnsignedInt, uint64(t.Height)); err != nil {
		return err
	}

	// t.Count (uint64) (uint64)

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajUnsignedInt, uint64(t.Count)); err != nil {
		return err
	}

	// t.Node (amt.Node) (struct)
	if err := t.Node.MarshalCBOR(w); err != nil {
		return err
	}
	return nil
}

func (t *Root) UnmarshalCBOR(r io.Reader) error {
	*t = Root{}

	br := cbg.GetPeeker(r)
	scratch := make([]byte, 8)

	maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 3 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.Height (uint64) (uint64)

	{

		maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
		if err != nil {
			return err
		}
		if maj != cbg.MajUnsignedInt {
			return fmt.Errorf("wrong type for uint64 field")
		}
		t.Height = uint64(extra)

	}
	// t.Count (uint64) (uint64)

	{

		maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
		if err != nil {
			return err
		}
		if maj != cbg.MajUnsignedInt {
			return fmt.Errorf("wrong type for uint64 field")
		}
		t.Count = uint64(extra)

	}
	// t.Node (amt.Node) (struct)

	{

		if err := t.Node.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.Node: %w", err)
		}

	}
	return nil
}

var lengthBufNode = []byte{131}

func (t *Node) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write(lengthBufNode); err != nil {
		return err
	}

	scratch := make([]byte, 9)

	// t.Bmap ([]uint8) (slice)
	if len(t.Bmap) > cbg.ByteArrayMaxLen {
		return xerrors.Errorf("Byte array in field t.Bmap was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajByteString, uint64(len(t.Bmap))); err != nil {
		return err
	}

	if _, err := w.Write(t.Bmap); err != nil {
		return err
	}

	// t.Links ([]cid.Cid) (slice)
	if len(t.Links) > cbg.MaxLength {
		return xerrors.Errorf("Slice value in field t.Links was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajArray, uint64(len(t.Links))); err != nil {
		return err
	}
	for _, v := range t.Links {
		if err := cbg.WriteCidBuf(scratch, w, v); err != nil {
			return xerrors.Errorf("failed writing cid field t.Links: %w", err)
		}
	}

	// t.Values ([]*typegen.Deferred) (slice)
	if len(t.Values) > cbg.MaxLength {
		return xerrors.Errorf("Slice value in field t.Values was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajArray, uint64(len(t.Values))); err != nil {
		return err
	}
	for _, v := range t.Values {
		if err := v.MarshalCBOR(w); err != nil {
			return err
		}
	}
	return nil
}

func (t *Node) UnmarshalCBOR(r io.Reader) error {
	*t = Node{}

	br := cbg.GetPeeker(r)
	scratch := make([]byte, 8)

	maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 3 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.Bmap ([]uint8) (slice)

	maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}

	if extra > cbg.ByteArrayMaxLen {
		return fmt.Errorf("t.Bmap: byte array too large (%d)", extra)
	}
	if maj != cbg.MajByteString {
		return fmt.Errorf("expected byte array")
	}
	t.Bmap = make([]byte, extra)
	if _, err := io.ReadFull(br, t.Bmap); err != nil {
		return err
	}
	// t.Links ([]cid.Cid) (slice)

	maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("t.Links: array too large (%d)", extra)
	}

	if maj != cbg.MajArray {
		return fmt.Errorf("expected cbor array")
	}

	if extra > 0 {
		t.Links = make([]cid.Cid, extra)
	}

	for i := 0; i < int(extra); i++ {

		c, err := cbg.ReadCid(br)
		if err != nil {
			return xerrors.Errorf("reading cid field t.Links failed: %w", err)
		}
		t.Links[i] = c
	}

	// t.Values ([]*typegen.Deferred) (slice)

	maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("t.Values: array too large (%d)", extra)
	}

	if maj != cbg.MajArray {
		return fmt.Errorf("expected cbor array")
	}

	if extra > 0 {
		t.Values = make([]*cbg.Deferred, extra)
	}

	for i := 0; i < int(extra); i++ {

		var v cbg.Deferred
		if err := v.UnmarshalCBOR(br); err != nil {
			return err
		}

		t.Values[i] = &v
	}

	return nil
}
