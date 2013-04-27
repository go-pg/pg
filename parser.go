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
	start := p.pos

	prev := p.Next()
	if prev != '"' {
		p.pos--
		bb := p.ReadSep([]byte{','})
		if bytes.Equal(bb, pgNull) {
			return nil
		}
		return bb
	}

	for p.Valid() {
		c := p.Next()
		if c == '"' && prev != '\\' {
			bb := p.b[start+1 : p.pos-1]
			if p.Valid() {
				// Read ",".
				p.pos += 1
			}
			return bb
		}
		prev = c
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

func (p *hstoreParser) NextKey() []byte {
	start := p.pos

	prev := p.Next()
	if prev != '"' {
		p.pos--
		bb := p.ReadSep([]byte{'=', '>'})
		if bytes.Equal(bb, pgNull) {
			return nil
		}
		return bb
	}

	for p.Valid() {
		c := p.Next()
		if c == '"' && prev != '\\' {
			bb := p.b[start+1 : p.pos-1]
			if p.Valid() {
				// Read "=>".
				p.pos += 2
			}
			return bb
		}
		prev = c
	}

	p.err = fmt.Errorf("pg: can't parse hstore: %q", p.b)
	return nil
}

func (p *hstoreParser) NextValue() []byte {
	start := p.pos

	prev := p.Next()
	if prev != '"' {
		p.pos--
		bb := p.ReadSep([]byte{',', ' '})
		if bytes.Equal(bb, pgNull) {
			return nil
		}
		return bb
	}

	for p.Valid() {
		c := p.Next()
		if c == '"' && prev != '\\' {
			bb := p.b[start+1 : p.pos-1]
			if p.Valid() {
				// Read ",".
				p.pos += 1
			}
			return bb
		}
		prev = c
	}

	p.err = fmt.Errorf("pg: can't parse hstore: %q", p.b)
	return nil
}
