package types

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/go-pg/pg/internal/parser"
)

var endOfArray = errors.New("pg: end of array")

type arrayParser struct {
	p parser.StreamingParser

	stickyErr error
}

func newArrayParserErr(err error) *arrayParser {
	return &arrayParser{
		stickyErr: err,
	}
}

func newArrayParser(rd Reader) *arrayParser {
	p := parser.NewStreamingParser(rd)
	err := p.SkipByte('{')
	if err != nil {
		return newArrayParserErr(err)
	}
	return &arrayParser{
		p: p,
	}
}

func (p *arrayParser) NextElem() ([]byte, error) {
	if p.stickyErr != nil {
		return nil, p.stickyErr
	}

	c, err := p.p.ReadByte()
	if err != nil {
		if err == io.EOF {
			return nil, endOfArray
		}
		return nil, err
	}

	switch c {
	case '"':
		b, err := p.p.ReadSubstring()
		if err != nil {
			return nil, err
		}
		err = p.readCommaBrace()
		if err != nil {
			return nil, err
		}
		return b, nil
	case '{':
		b, err := p.readSubArray()
		if err != nil {
			return nil, err
		}
		err = p.readCommaBrace()
		if err != nil {
			return nil, err
		}
		return b, nil
	case '}':
		return nil, endOfArray
	default:
		err = p.p.UnreadByte()
		if err != nil {
			return nil, err
		}

		var b []byte
		for {
			tmp, err := p.p.ReadSlice(',')
			if err == nil {
				if b == nil {
					b = tmp
				} else {
					b = append(b, tmp...)
				}
				b = b[:len(b)-1]
				break
			}
			b = append(b, tmp...)
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

		if c == '}' {
			b = append(b, '}')
			return b, nil
		}

		if c == '"' {
			b = append(b, '"')
			for {
				tmp, err := p.p.ReadSlice('"')
				b = append(b, tmp...)
				if err != nil {
					if err == bufio.ErrBufferFull {
						continue
					}
					return nil, err
				}
				if len(b) > 1 && b[len(b)-2] != '\\' {
					break
				}
			}
			continue
		}

		b = append(b, c)
	}
}

func (p *arrayParser) readCommaBrace() error {
	c, err := p.p.ReadByte()
	if err != nil {
		return err
	}
	switch c {
	case ',', '}':
		return nil
	default:
		return fmt.Errorf("pg: got %q, wanted ',' or '}'", c)
	}
}
