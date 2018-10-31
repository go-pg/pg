package types

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

var endOfArray = errors.New("types: end of array")

type arrayParser struct {
	p streamingParser

	valid     bool
	stickyErr error
}

func newArrayParser(rd Reader) *arrayParser {
	c, err := rd.ReadByte()
	if err != nil {
		return &arrayParser{
			stickyErr: err,
		}
	}

	if c != '{' {
		return &arrayParser{
			stickyErr: fmt.Errorf("pg: expecting '{', got %q", c),
		}
	}

	return &arrayParser{
		p: newStreamingParser(rd),

		stickyErr: err,
	}
}

func (p *arrayParser) Valid() bool {
	if p.stickyErr != nil {
		return false
	}
	return p.p.Available() > 0
}

func (p *arrayParser) NextElem() ([]byte, error) {
	if p.stickyErr != nil {
		return nil, p.stickyErr
	}

	c, err := p.p.ReadByte()
	if err != nil {
		return nil, err
	}

	switch c {
	case '"':
		b, err := p.p.ReadSubstring()
		if err != nil {
			return nil, err
		}

		c, err := p.p.ReadByte()
		if err != nil {
			return nil, err
		}

		switch c {
		case ',':
			return b, nil
		case '}':
			return b, nil
		default:
			return nil, fmt.Errorf("pg: got %q, wanted ',' or '}'", c)
		}
	case '{':
		b, err := p.readSubArray()
		if err != nil {
			return nil, err
		}

		c, err := p.p.ReadByte()
		if err != nil {
			return nil, err
		}

		switch c {
		case ',':
			return b, nil
		case '}':
			return b, nil
		default:
			return nil, fmt.Errorf("pg: got %q, wanted ',' or '}'", c)
		}
	case '}':
		return nil, endOfArray
	default:
		var b []byte
		b = append(b, c)

		for {
			bb, err := p.p.ReadSlice(',')
			b = append(b, bb...)
			if err == nil {
				b = b[:len(b)-1]
				break
			}
			if err == bufio.ErrBufferFull {
				continue
			}
			if err == io.EOF {
				if b[len(b)-1] == '}' {
					b = b[:len(b)-1]
					break
				}
			}
			return nil, err
		}

		if bytes.Equal(b, []byte("NULL")) {
			return nil, nil
		}

		return b, nil
	}
}

func (p *arrayParser) readSubArray() ([]byte, error) {
	var b []byte
	b = append(b, '{')
	for {
		c, err := p.p.ReadByte()
		if err != nil {
			return nil, err
		}

		switch c {
		case '"':
			b = append(b, '"')
			for {
				bb, err := p.p.ReadSlice('"')
				b = append(b, bb...)
				if err != nil {
					return nil, err
				}

				if len(b) > 1 && b[len(b)-2] != '\\' {
					break
				}
			}
		case '}':
			b = append(b, '}')
			return b, nil
		default:
			b = append(b, c)
		}
	}
}
