package pool

import (
	"encoding/binary"
	"io"
)

var nullParamLength = int32(-1)

type Buffer struct {
	w     io.Writer
	Bytes []byte
	start []int // Message start position.
}

func NewBuffer(w io.Writer, b []byte) *Buffer {
	return &Buffer{
		w:     w,
		Bytes: b,
	}
}

func (buf *Buffer) StartMessage(c byte) {
	if c == 0 {
		buf.start = append(buf.start, len(buf.Bytes))
		buf.Bytes = append(buf.Bytes, 0, 0, 0, 0)
	} else {
		buf.start = append(buf.start, len(buf.Bytes)+1)
		buf.Bytes = append(buf.Bytes, c, 0, 0, 0, 0)
	}
}

func (buf *Buffer) popStart() int {
	start := buf.start[len(buf.start)-1]
	buf.start = buf.start[:len(buf.start)-1]
	return start
}

func (buf *Buffer) FinishMessage() {
	start := buf.popStart()
	binary.BigEndian.PutUint32(buf.Bytes[start:], uint32(len(buf.Bytes)-start))
}

func (buf *Buffer) StartParam() {
	buf.StartMessage(0)
}

func (buf *Buffer) FinishParam() {
	start := buf.popStart()
	binary.BigEndian.PutUint32(buf.Bytes[start:], uint32(len(buf.Bytes)-start-4))
}

func (buf *Buffer) FinishNullParam() {
	start := buf.popStart()
	binary.BigEndian.PutUint32(buf.Bytes[start:], uint32(nullParamLength))
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
	if len(buf.start) != 0 {
		panic("message was not finished")
	}

	b := buf.Bytes[:]
	buf.Bytes = buf.Bytes[:0]

	_, err := buf.w.Write(b)
	return err
}

func (buf *Buffer) Reset() {
	buf.start = buf.start[:0]
	buf.Bytes = buf.Bytes[:0]
}

// TODO: what???
func (buf *Buffer) ReadFrom(r io.Reader) (int64, error) {
	n, err := r.Read(buf.Bytes[len(buf.Bytes):cap(buf.Bytes)])
	buf.Bytes = buf.Bytes[:len(buf.Bytes)+int(n)]
	return int64(n), err
}
