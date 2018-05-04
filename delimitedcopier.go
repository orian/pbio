package pbio

import(
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

func (this *DelimitedCopier) CopyMsg(w io.Writer) error {
	length64, err := binary.ReadUvarint(this.r)
	if err != nil {
		return err
	}
	length := int(length64)
	if length < 0 || length > this.maxSize {
		return io.ErrShortBuffer
	}
	if len(this.buf) < length {
		this.buf = make([]byte, length)
	}
	buf := this.buf[:length]
	if _, err := io.ReadFull(this.r, buf); err != nil {
		return err
	}

	n := binary.PutUvarint(this.lenBuf, length64)
	_, err = w.Write(this.lenBuf[:n])
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

func (this *DelimitedCopier) SkipOne() error {
	length64, err := binary.ReadUvarint(this.r)
	if err != nil {
		return err
	}
	length := int(length64)
	if length < 0 || length > this.maxSize {
		return io.ErrShortBuffer
	}
	if len(this.buf) < length {
		this.buf = make([]byte, length)
	}
	buf := this.buf[:length]
	if _, err := io.ReadFull(this.r, buf); err != nil {
		return err
	}
	return nil
}

func (this *DelimitedCopier) Close() error {
	if this.closer != nil {
		return this.closer.Close()
	}
	return nil
}

// fulfil io.Reader
func (this *DelimitedCopier) Read(b []byte) (n int, err error) {
	return this.r.Read(b)
}

func NewDelimitedCopier(r io.Reader, maxSize int) *DelimitedCopier {
	var closer io.Closer
	if c, ok := r.(io.Closer); ok {
		closer = c
	}
	return &DelimitedCopier{bufio.NewReader(r), make([]byte, 10), nil, maxSize, closer}
}