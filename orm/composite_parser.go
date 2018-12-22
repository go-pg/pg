package orm

import (
	"bufio"
	"errors"
	"fmt"
	"io"

	"github.com/go-pg/pg/types"
)

var endOfComposite = errors.New("end of composite")

type compositeParser struct {
	p types.Reader

	stickyErr error
}

func newCompositeParserErr(err error) *compositeParser {
	return &compositeParser{
		stickyErr: err,
	}
}

func newCompositeParser(rd types.Reader) *compositeParser {
	c, err := rd.ReadByte()
	if err != nil {
		return newCompositeParserErr(err)
	}
	if c != '(' {
		err := fmt.Errorf("pg: got %q, wanted '('", c)
		return newCompositeParserErr(err)
	}
	return &compositeParser{
		p: rd,
	}
}

func (p *compositeParser) NextElem() ([]byte, error) {
	if p.stickyErr != nil {
		return nil, p.stickyErr
	}

	c, err := p.p.ReadByte()
	if err != nil {
		if err == io.EOF {
			return nil, endOfComposite
		}
		return nil, err
	}

	switch c {
	case '"':
		return p.readQuoted()
	case ',':
		return p.NextElem()
	case ')':
		return nil, endOfComposite
	default:
		_ = p.p.UnreadByte()
	}

	var b []byte
	for {
		bb, err := p.p.ReadSlice(',')
		if b == nil {
			b = bb[:len(bb):len(bb)]
		} else {
			b = append(b, bb...)
		}
		if err == nil {
			b = b[:len(b)-1]
			break
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if err == io.EOF {
			if b[len(b)-1] == ')' {
				b = b[:len(b)-1]
				break
			}
		}
		return nil, err
	}

	if len(b) == 0 { // NULL
		return nil, nil
	}
	return b, nil
}

func (p *compositeParser) readQuoted() ([]byte, error) {
	var b []byte
	for {
		c, err := p.p.ReadByte()
		if err != nil {
			return nil, err
		}

		switch c {
		case '\\':
			c2, err := p.p.ReadByte()
			if err != nil {
				return nil, err
			}
			switch c2 {
			case '\\':
				b = append(b, '\\')
			default:
				_ = p.p.UnreadByte()
				b = append(b, c)
			}
		case '\'':
			c2, err := p.p.ReadByte()
			if err != nil {
				return nil, err
			}
			switch c2 {
			case '\'':
				b = append(b, '\'')
			default:
				_ = p.p.UnreadByte()
				b = append(b, c)
			}
		case '"':
			c2, err := p.p.ReadByte()
			if err != nil {
				return nil, err
			}
			switch c2 {
			case '"':
				b = append(b, '"')
			default:
				return b, nil
			}
		default:
			b = append(b, c)
		}
	}
}
