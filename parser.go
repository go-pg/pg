package pg

import (
	"bytes"
	"fmt"
)

var (
	pgNull = []byte("NULL")
)

type parser struct {
	b   []byte
	pos int
	err error
}

func (p *parser) Err() error {
	return p.err
}

func (p *parser) Valid() bool {
	return p.err == nil && p.pos < len(p.b)
}

func (p *parser) Next() byte {
	c := p.b[p.pos]
	p.pos++
	return c
}

func (p *parser) Peek() byte {
	if p.Valid() {
		return p.b[p.pos]
	}
	return 0
}

func (p *parser) SkipNext() {
	p.pos++
}

func (p *parser) ReadSep(sep []byte) []byte {
	start := p.pos
	end := bytes.Index(p.b[start:], sep)
	if end >= 0 {
		p.pos += end + len(sep)
		return p.b[start : p.pos-len(sep)]
	}
	p.pos = len(p.b)
	return p.b[start:p.pos]
}

type arrayParser struct {
	*parser
}

func newArrayParser(b []byte) *arrayParser {
	return &arrayParser{
		parser: &parser{b: b},
	}
}

func (p *arrayParser) NextElem() []byte {
	if p.Next() != '"' {
		p.pos--
		b := p.ReadSep([]byte{','})
		if bytes.Equal(b, pgNull) {
			return nil
		}
		return b
	}

	b := make([]byte, 0)
	for p.Valid() {
		c := p.Next()
		switch c {
		case '\\':
			switch p.Peek() {
			case '\\':
				b = append(b, '\\')
				p.SkipNext()
			case '"':
				b = append(b, '"')
				p.SkipNext()
			default:
				b = append(b, c)
			}
		case '\'':
			switch p.Peek() {
			case '\'':
				b = append(b, '\'')
				p.SkipNext()
			default:
				b = append(b, c)
			}
		case '"':
			// Read ",".
			p.pos++
			return b
		default:
			b = append(b, c)
		}
	}

	p.err = fmt.Errorf("pg: can't parse array: %q", p.b)
	return nil
}

type hstoreParser struct {
	*parser
}

func newHstoreParser(b []byte) *hstoreParser {
	return &hstoreParser{
		parser: &parser{b: b},
	}
}

func (p *hstoreParser) NextKey() ([]byte, error) {
	next := p.Next()
	if next == ',' {
		if n := p.Peek(); n == ' ' {
			p.pos++
		}
		next = p.Next()
	}

	if next != '"' {
		p.pos--
		bb := p.ReadSep([]byte{'=', '>'})
		if bytes.Equal(bb, pgNull) {
			return nil, nil
		}
		return bb, nil
	}

	b := make([]byte, 0)
	for p.Valid() {
		c := p.Next()
		switch c {
		case '\\':
			switch p.Peek() {
			case '\\':
				b = append(b, '\\')
				p.SkipNext()
			case '"':
				b = append(b, '"')
				p.SkipNext()
			default:
				b = append(b, c)
			}
		case '\'':
			switch p.Peek() {
			case '\'':
				b = append(b, '\'')
				p.SkipNext()
			default:
				b = append(b, c)
			}
		case '"':
			// Read "=>".
			p.pos += 2
			return b, nil
		default:
			b = append(b, c)
		}
	}

	return nil, fmt.Errorf("pg: can't parse hstore: %s", p.b)
}

func (p *hstoreParser) NextValue() ([]byte, error) {
	if p.Next() != '"' {
		p.pos--
		bb := p.ReadSep([]byte{',', ' '})
		if bytes.Equal(bb, pgNull) {
			return nil, nil
		}
		return bb, nil
	}

	b := make([]byte, 0)
	for p.Valid() {
		c := p.Next()
		switch c {
		case '\\':
			switch p.Peek() {
			case '\\':
				b = append(b, '\\')
				p.SkipNext()
			case '"':
				b = append(b, '"')
				p.SkipNext()
			default:
				b = append(b, c)
			}
		case '\'':
			switch p.Peek() {
			case '\'':
				b = append(b, '\'')
				p.SkipNext()
			default:
				b = append(b, c)
			}
		case '"':
			return b, nil
		default:
			b = append(b, c)
		}
	}

	return nil, fmt.Errorf("pg: can't parse hstore: %s", p.b)
}
