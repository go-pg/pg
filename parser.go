package pg

import (
	"bytes"
	"strconv"
)

var (
	pgNull = []byte("NULL")
)

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func isAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isAlnum(c byte) bool {
	return isAlpha(c) || isDigit(c)
}

type parser struct {
	b   []byte
	pos int
	err error
}

func (p *parser) Valid() bool {
	return p.pos < len(p.b)
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

func (p *parser) ReadName() string {
	start := p.pos
	for p.Valid() {
		ch := p.Next()
		if !(isAlnum(ch) || ch == '_') {
			p.pos--
			break
		}
	}
	return string(p.b[start:p.pos])
}

func (p *parser) ReadNumber() int {
	start := p.pos
	for p.Valid() {
		if !isDigit(p.Next()) {
			p.pos--
			break
		}
	}
	n, _ := strconv.Atoi(string(p.b[start:p.pos]))
	return n
}

type arrayParser struct {
	*parser

	err error
}

func newArrayParser(b []byte) *arrayParser {
	var err error
	if len(b) < 2 || b[0] != '{' || b[len(b)-1] != '}' {
		err = errorf("pg: can't parse string slice: %q", b)
	} else {
		b = b[1 : len(b)-1]
	}
	return &arrayParser{
		parser: &parser{b: b},

		err: err,
	}
}

func (p *arrayParser) NextElem() ([]byte, error) {
	if p.err != nil {
		return nil, p.err
	}

	if p.Next() != '"' {
		p.pos--
		b := p.ReadSep([]byte{','})
		if bytes.Equal(b, pgNull) {
			return nil, nil
		}
		return b, nil
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
			return b, nil
		default:
			b = append(b, c)
		}
	}

	return nil, errorf("pg: can't parse array: %q", p.b)
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

	return nil, errorf("pg: can't parse hstore: %s", p.b)
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

	return nil, errorf("pg: can't parse hstore: %s", p.b)
}
