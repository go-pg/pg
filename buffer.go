package pg

import (
	"encoding/binary"
	"io"
)

type buffer struct {
	B  []byte
	b8 []byte

	start int // Message start position.
}

func newBuffer() *buffer {
	return &buffer{
		B:  make([]byte, 0, 8192),
		b8: make([]byte, 8),
	}
}

func (buf *buffer) StartMessage(c msgType) {
	if len(buf.B) > 0 {
		buf.closeMessage()
	}

	if c == 0 {
		buf.start = len(buf.B)
		buf.B = append(buf.B, 0, 0, 0, 0)
	} else {
		buf.start = len(buf.B) + 1
		buf.B = append(buf.B, byte(c), 0, 0, 0, 0)
	}
}

func (buf *buffer) closeMessage() {
	binary.BigEndian.PutUint32(buf.B[buf.start:], uint32(len(buf.B)-buf.start))
}

func (buf *buffer) Grow(n int) {
	buf.B = append(buf.B, buf.b8[:n]...)
}

func (buf *buffer) Write(b []byte) (int, error) {
	buf.B = append(buf.B, b...)
	return len(b), nil
}

func (buf *buffer) WriteInt16(num int16) {
	buf.Grow(2)
	binary.BigEndian.PutUint16(buf.B[len(buf.B)-2:], uint16(num))
}

func (buf *buffer) WriteInt32(num int32) {
	buf.Grow(4)
	binary.BigEndian.PutUint32(buf.B[len(buf.B)-4:], uint32(num))
}

func (buf *buffer) WriteString(s string) {
	buf.B = append(buf.B, s...)
	buf.B = append(buf.B, 0)
}

func (buf *buffer) WriteBytes(b []byte) {
	buf.B = append(buf.B, b...)
	buf.B = append(buf.B, 0)
}

func (buf *buffer) WriteByte(c byte) {
	buf.B = append(buf.B, c)
}

func (buf *buffer) Flush() []byte {
	if len(buf.B) > 0 {
		buf.closeMessage()
	}

	b := buf.B[:]
	buf.B = buf.B[:0]
	return b
}

func (buf *buffer) Reset() {
	buf.B = buf.B[:0]
}

func (buf *buffer) ReadFrom(r io.Reader) (int64, error) {
	n, err := r.Read(buf.B[len(buf.B):cap(buf.B)])
	buf.B = buf.B[:len(buf.B)+int(n)]
	return int64(n), err
}
