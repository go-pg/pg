package parser

import (
	"bytes"
	"strconv"
)

func isNum(c byte) bool {
	return c >= '0' && c <= '9'
}

func isAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isAlnum(c byte) bool {
	return isAlpha(c) || isNum(c)
}

type Parser struct {
	b   []byte
	pos int
	err error
}

func New(b []byte) *Parser {
	return &Parser{b: b}
}

func (p *Parser) Bytes() []byte {
	return p.b[p.pos:]
}

func (p *Parser) Valid() bool {
	return p.pos < len(p.b)
}

func (p *Parser) Read() byte {
	c := p.b[p.pos]
	p.pos++
	return c
}

func (p *Parser) Peek() byte {
	if p.Valid() {
		return p.b[p.pos]
	}
	return 0
}

func (p *Parser) Skip(c byte) {
	p.pos++
}

func (p *Parser) Got(s string) bool {
	end := p.pos + len(s)
	if end <= len(p.b) && bytes.Equal(p.b[p.pos:end], []byte(s)) {
		p.pos = end
		return true
	}
	return false
}

func (p *Parser) ReadSep(sep []byte) []byte {
	start := p.pos
	end := bytes.Index(p.b[start:], sep)
	if end >= 0 {
		p.pos += end + len(sep)
		return p.b[start : p.pos-len(sep)]
	}
	p.pos = len(p.b)
	return p.b[start:p.pos]
}

func (p *Parser) ReadIdentifier() string {
	start := p.pos
	for p.Valid() {
		ch := p.Read()
		if !(isAlnum(ch) || ch == '_') {
			p.pos--
			break
		}
	}
	return string(p.b[start:p.pos])
}

func (p *Parser) ReadNumber() int {
	start := p.pos
	for p.Valid() {
		if !isNum(p.Read()) {
			p.pos--
			break
		}
	}
	n, _ := strconv.Atoi(string(p.b[start:p.pos]))
	return n
}
