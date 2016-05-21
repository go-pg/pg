package pool

import (
	"encoding/binary"
	"io"
)

type Buffer struct {
	w     io.Writer
	Bytes []byte

	msgStart, paramStart int
}

func NewBuffer(w io.Writer, b []byte) *Buffer {
	return &Buffer{
		w:     w,
		Bytes: b,
	}
}

func (buf *Buffer) StartMessage(c byte) {
	if c == 0 {
		buf.msgStart = len(buf.Bytes)
		buf.Bytes = append(buf.Bytes, 0, 0, 0, 0)
	} else {
		buf.msgStart = len(buf.Bytes) + 1
		buf.Bytes = append(buf.Bytes, c, 0, 0, 0, 0)
	}
}

func (buf *Buffer) FinishMessage() {
	binary.BigEndian.PutUint32(
		buf.Bytes[buf.msgStart:], uint32(len(buf.Bytes)-buf.msgStart))
}

func (buf *Buffer) StartParam() {
	buf.paramStart = len(buf.Bytes)
	buf.Bytes = append(buf.Bytes, 0, 0, 0, 0)
}

func (buf *Buffer) FinishParam() {
	binary.BigEndian.PutUint32(
		buf.Bytes[buf.paramStart:], uint32(len(buf.Bytes)-buf.paramStart-4))
}

var nullParamLength = int32(-1)

func (buf *Buffer) FinishNullParam() {
	binary.BigEndian.PutUint32(
		buf.Bytes[buf.paramStart:], uint32(nullParamLength))
}

func (buf *Buffer) Write(b []byte) (int, error) {
	buf.Bytes = append(buf.Bytes, b...)
	return len(b), nil
}

func (buf *Buffer) WriteInt16(num int16) {
	buf.Bytes = append(buf.Bytes, 0, 0)
	binary.BigEndian.PutUint16(buf.Bytes[len(buf.Bytes)-2:], uint16(num))
}

func (buf *Buffer) WriteInt32(num int32) {
	buf.Bytes = append(buf.Bytes, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(buf.Bytes[len(buf.Bytes)-4:], uint32(num))
}

func (buf *Buffer) WriteString(s string) {
	buf.Bytes = append(buf.Bytes, s...)
	buf.Bytes = append(buf.Bytes, 0)
}

func (buf *Buffer) WriteBytes(b []byte) {
	buf.Bytes = append(buf.Bytes, b...)
	buf.Bytes = append(buf.Bytes, 0)
}

func (buf *Buffer) WriteByte(c byte) {
	buf.Bytes = append(buf.Bytes, c)
}

func (buf *Buffer) Flush() error {
	_, err := buf.w.Write(buf.Bytes)
	buf.Bytes = buf.Bytes[:0]
	return err
}

func (buf *Buffer) Reset() {
	buf.Bytes = buf.Bytes[:0]
}

func (buf *Buffer) ReadFrom(r io.Reader) (int64, error) {
	n, err := r.Read(buf.Bytes[len(buf.Bytes):cap(buf.Bytes)])
	buf.Bytes = buf.Bytes[:len(buf.Bytes)+int(n)]
	return int64(n), err
}
