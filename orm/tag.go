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

	tag     tag
	hasName bool
	key     string
}

func (p *tagParser) setName(name string) {
	if p.hasName || p.tag.Options != nil {
		p.setTagOption(name, "")
		return
	}
	p.hasName = true
	p.tag.Name = name
}

func (p *tagParser) setTagOption(key, value string) {
	if p.tag.Options == nil {
		p.tag.Options = make(map[string]string)
	}
	p.tag.Options[key] = value
}

func (p *tagParser) parseKey() {
	p.key = ""

	var b []byte
	for p.Valid() {
		c := p.Read()
		switch c {
		case ',':
			p.Skip(' ')
			p.setName(string(b))
			p.parseKey()
			return
		case ':':
			p.key = string(b)
			p.parseValue()
			return
		default:
			b = append(b, c)
		}
	}

	if len(b) > 0 {
		p.setName(string(b))
	}
}

func (p *tagParser) parseValue() {
	const quote = '\''

	c := p.Peek()
	if c == quote {
		p.parseQuotedValue()
		return
	}

	var b []byte
	for p.Valid() {
		c = p.Read()
		switch c {
		case '\\':
			c = p.Read()
			b = append(b, c)
		case ',':
			p.Skip(' ')
			p.setTagOption(p.key, string(b))
			p.parseKey()
			return
		default:
			b = append(b, c)
		}
	}
	p.setTagOption(p.key, string(b))
}

func (p *tagParser) parseQuotedValue() {
	const quote = '\''

	if !p.Skip(quote) {
		panic("not reached")
	}

	var b []byte
	b = append(b, quote)

	for p.Valid() {
		bb, ok := p.ReadSep(quote)
		if !ok {
			b = append(b, bb...)
			break
		}

		if len(bb) > 0 && bb[len(bb)-1] == '\\' {
			b = append(b, bb[:len(bb)-1]...)
			b = append(b, quote)
			continue
		}

		b = append(b, bb...)
		b = append(b, quote)
		break
	}

	p.setTagOption(p.key, string(b))
	if p.Skip(',') {
		p.Skip(' ')
	}
	p.parseKey()
}

func unquote(s string) (string, bool) {
	const quote = '\''

	if len(s) < 2 {
		return s, false
	}
	if s[0] == quote && s[len(s)-1] == quote {
		return s[1 : len(s)-1], true
	}
	return s, false
}
