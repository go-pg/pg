package pg

import (
	"encoding/binary"
	"io"
)

var nullParamLength = int32(-1)

type buffer struct {
	Bytes []byte
	start []int // Message start position.
}

func newBuffer() *buffer {
	return &buffer{
		Bytes: make([]byte, 0, 8192),
	}
}

func (buf *buffer) StartMessage(c msgType) {
	if c == 0 {
		buf.start = append(buf.start, len(buf.Bytes))
		buf.Bytes = append(buf.Bytes, 0, 0, 0, 0)
	} else {
		buf.start = append(buf.start, len(buf.Bytes)+1)
		buf.Bytes = append(buf.Bytes, byte(c), 0, 0, 0, 0)
	}
}

func (buf *buffer) StartParam() {
	buf.StartMessage(0)
}

func (buf *buffer) popStart() int {
	start := buf.start[len(buf.start)-1]
	buf.start = buf.start[:len(buf.start)-1]
	return start
}

func (buf *buffer) FinishMessage() {
	start := buf.popStart()
	binary.BigEndian.PutUint32(buf.Bytes[start:], uint32(len(buf.Bytes)-start))
}

func (buf *buffer) FinishParam() {
	start := buf.popStart()
	binary.BigEndian.PutUint32(buf.Bytes[start:], uint32(len(buf.Bytes)-start-4))
}

func (buf *buffer) FinishNullParam() {
	start := buf.popStart()
	binary.BigEndian.PutUint32(buf.Bytes[start:], uint32(nullParamLength))
}

func (buf *buffer) Write(b []byte) (int, error) {
	buf.Bytes = append(buf.Bytes, b...)
	return len(b), nil
}

func (buf *buffer) WriteInt16(num int16) {
	buf.Bytes = append(buf.Bytes, 0, 0)
	binary.BigEndian.PutUint16(buf.Bytes[len(buf.Bytes)-2:], uint16(num))
}

func (buf *buffer) WriteInt32(num int32) {
	buf.Bytes = append(buf.Bytes, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(buf.Bytes[len(buf.Bytes)-4:], uint32(num))
}

func (buf *buffer) WriteString(s string) {
	buf.Bytes = append(buf.Bytes, s...)
	buf.Bytes = append(buf.Bytes, 0)
}

func (buf *buffer) WriteBytes(b []byte) {
	buf.Bytes = append(buf.Bytes, b...)
	buf.Bytes = append(buf.Bytes, 0)
}

func (buf *buffer) WriteByte(c byte) {
	buf.Bytes = append(buf.Bytes, c)
}

func (buf *buffer) Flush() []byte {
	if len(buf.start) != 0 {
		panic("message was not finished")
	}

	b := buf.Bytes[:]
	buf.Bytes = buf.Bytes[:0]
	return b
}

func (buf *buffer) Reset() {
	buf.start = buf.start[:0]
	buf.Bytes = buf.Bytes[:0]
}

func (buf *buffer) ReadFrom(r io.Reader) (int64, error) {
	n, err := r.Read(buf.Bytes[len(buf.Bytes):cap(buf.Bytes)])
	buf.Bytes = buf.Bytes[:len(buf.Bytes)+int(n)]
	return int64(n), err
}
