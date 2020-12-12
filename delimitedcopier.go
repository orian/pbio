package pbio

import (
	"bufio"
	"encoding/binary"
	"io"
)

// Copied from: github.com/gogo/protobuf/io/varint.go
type DelimitedCopier struct {
	r       *bufio.Reader
	lenBuf  []byte
	buf     []byte
	maxSize int
	closer  io.Closer
}

func (d *DelimitedCopier) CopyMsg(w io.Writer) error {
	length64, err := binary.ReadUvarint(d.r)
	if err != nil {
		return err
	}
	length := int(length64)
	if length < 0 || length > d.maxSize {
		return io.ErrShortBuffer
	}
	if len(d.buf) < length {
		d.buf = make([]byte, length)
	}
	buf := d.buf[:length]
	if _, err := io.ReadFull(d.r, buf); err != nil {
		return err
	}

	n := binary.PutUvarint(d.lenBuf, length64)
	_, err = w.Write(d.lenBuf[:n])
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

func (d *DelimitedCopier) SkipOne() error {
	length64, err := binary.ReadUvarint(d.r)
	if err != nil {
		return err
	}
	length := int(length64)
	if length < 0 || length > d.maxSize {
		return io.ErrShortBuffer
	}
	if len(d.buf) < length {
		d.buf = make([]byte, length)
	}
	buf := d.buf[:length]
	if _, err := io.ReadFull(d.r, buf); err != nil {
		return err
	}
	return nil
}

func (d *DelimitedCopier) Close() error {
	if d.closer != nil {
		return d.closer.Close()
	}
	return nil
}

// fulfil io.Reader
func (d *DelimitedCopier) Read(b []byte) (n int, err error) {
	return d.r.Read(b)
}

func NewDelimitedCopier(r io.Reader, maxSize int) *DelimitedCopier {
	var closer io.Closer
	if c, ok := r.(io.Closer); ok {
		closer = c
	}
	return &DelimitedCopier{bufio.NewReader(r), make([]byte, 10), nil, maxSize, closer}
}
