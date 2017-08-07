package orm

import (
	"github.com/go-pg/pg/internal/parser"
)

type tag struct {
	Name    string
	Options map[string]string
}

func parseTag(s string) *tag {
	p := &tagParser{
		Parser: parser.NewString(s),
	}
	p.parseKey()
	return &p.tag
}

type tagParser struct {
	*parser.Parser

	tag tag
	key string
}

func (p *tagParser) setTagOption(key, value string) {
	if p.tag.Options == nil {
		p.tag.Options = make(map[string]string)
	}
	p.tag.Options[key] = value
}

func (p *tagParser) parseKey() {
	start := p.Position()
	end := minIndex(p.PositionByte(','), p.PositionByte(':'))
	b := p.Slice(start, end)
	c := p.Read()

	if c == ':' {
		p.key = string(b)
		p.parseValue()
		return
	}

	if start == 0 {
		p.tag.Name = string(b)
	} else {
		p.setTagOption(string(b), "")
	}

	if p.Valid() {
		p.parseKey()
	}
}

func (p *tagParser) parseValue() {
	c := p.Peek()
	quote := c == '\''

	start := p.Position()
	if quote {
		start++
		p.Advance()
	}

	var end int
	if quote {
		end = p.PositionByte('\'')
	} else {
		end = p.PositionByte(',')
	}

	value := p.Slice(start, end)
	p.setTagOption(p.key, string(value))

	if quote {
		p.Skip('\'')
	}

	if p.Valid() {
		p.parseKey()
	}
}

func minIndex(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
