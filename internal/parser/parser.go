package parser

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/go-pg/pg/internal"
)

type Parser struct {
	b []byte
}

func New(b []byte) *Parser {
	return &Parser{
		b: b,
	}
}

func NewString(s string) *Parser {
	return New(internal.StringToBytes(s))
}

func (p *Parser) Bytes() []byte {
	return p.b
}

func (p *Parser) Valid() bool {
	return len(p.b) > 0
}

func (p *Parser) Read() byte {
	if p.Valid() {
		c := p.b[0]
		p.Advance()
		return c
	}
	return 0
}

func (p *Parser) Peek() byte {
	if p.Valid() {
		return p.b[0]
	}
	return 0
}

func (p *Parser) Advance() {
	p.b = p.b[1:]
}

func (p *Parser) Skip(c byte) bool {
	if p.Peek() == c {
		p.Advance()
		return true
	}
	return false
}

func (p *Parser) MustSkip(c byte) error {
	if p.Skip(c) {
		return nil
	}
	return fmt.Errorf("expecting '%c', got %q", c, p.Bytes())
}

func (p *Parser) SkipBytes(b []byte) bool {
	if len(b) > len(p.b) {
		return false
	}
	if !bytes.Equal(p.b[:len(b)], b) {
		return false
	}
	p.b = p.b[len(b):]
	return true
}

func (p *Parser) ReadSep(c byte) ([]byte, bool) {
	ind := bytes.IndexByte(p.b, c)
	if ind == -1 {
		b := p.b
		p.b = p.b[len(p.b):]
		return b, false
	}

	b := p.b[:ind]
	p.b = p.b[ind+1:]
	return b, true
}

func (p *Parser) ReadIdentifier() (s string, numeric bool) {
	end := len(p.b)
	numeric = true
	for i, ch := range p.b {
		if isNum(ch) {
			continue
		}
		if isAlpha(ch) || (i > 0 && ch == '_') {
			numeric = false
			continue
		}
		end = i
		break
	}
	if end == 0 {
		return "", false
	}
	b := p.b[:end]
	p.b = p.b[end:]
	return internal.BytesToString(b), numeric
}

func (p *Parser) ReadNumber() int {
	end := len(p.b)
	for i, ch := range p.b {
		if !isNum(ch) {
			end = i
			break
		}
	}
	if end <= 0 {
		return 0
	}
	n, _ := strconv.Atoi(string(p.b[:end]))
	p.b = p.b[end:]
	return n
}

func (p *Parser) ReadString() ([]byte, error) {
	quote := p.Read()
	b, ok := p.ReadSep(quote)
	if !ok {
		return nil, fmt.Errorf("can't find closing quote")
	}
	return b, nil
}

func (p *Parser) readSubstring() []byte {
	var b []byte
	for p.Valid() {
		c := p.Read()
		switch c {
		case '\\':
			switch p.Peek() {
			case '\\':
				b = append(b, '\\')
				p.Advance()
			case '"':
				b = append(b, '"')
				p.Advance()
			default:
				b = append(b, c)
			}
		case '\'':
			switch p.Peek() {
			case '\'':
				b = append(b, '\'')
				p.Advance()
			default:
				b = append(b, c)
			}
		case '"':
			return b
		default:
			b = append(b, c)
		}
	}
	return b
}
