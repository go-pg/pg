package pool

import (
	"encoding/binary"

	"github.com/go-pg/pg/internal"
)

type Reader struct {
	*ElasticBufReader
	Columns [][]byte
}

func NewReader(buf *ElasticBufReader) *Reader {
	return &Reader{ElasticBufReader: buf}
}

func (rd *Reader) ReadInt16() (int16, error) {
	b, err := rd.ReadN(2)
	if err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(b)), nil
}

func (rd *Reader) ReadInt32() (int32, error) {
	b, err := rd.ReadN(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b)), nil
}

func (rd *Reader) ReadString() (string, error) {
	b, err := rd.ReadSlice(0)
	if err != nil {
		return "", err
	}
	return string(b[:len(b)-1]), nil
}

func (rd *Reader) ReadError() (error, error) {
	m := make(map[byte]string)
	for {
		c, err := rd.ReadByte()
		if err != nil {
			return nil, err
		}
		if c == 0 {
			break
		}
		s, err := rd.ReadString()
		if err != nil {
			return nil, err
		}
		m[c] = s
	}
	return internal.NewPGError(m), nil
}

func (rd *Reader) ReadMessageType() (byte, int, error) {
	c, err := rd.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	l, err := rd.ReadInt32()
	if err != nil {
		return 0, 0, err
	}
	return c, int(l) - 4, nil
}
