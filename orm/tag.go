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

func (p *tagParser) setTagOption(key, value string) {
	if !p.hasName {
		p.hasName = true
		if key == "" {
			p.tag.Name = value
			return
		}
	}
	if p.tag.Options == nil {
		p.tag.Options = make(map[string]string)
	}
	if key == "" {
		p.tag.Options[value] = ""
	} else {
		p.tag.Options[key] = value
	}
}

func (p *tagParser) parseKey() {
	p.key = ""

	var b []byte
	for p.Valid() {
		c := p.Read()
		switch c {
		case ',':
			p.Skip(' ')
			p.setTagOption("", string(b))
			p.parseKey()
			return
		case ':':
			p.key = string(b)
			p.parseValue()
			return
		case '\'':
			p.parseQuotedValue()
			return
		default:
			b = append(b, c)
		}
	}

	if len(b) > 0 {
		p.setTagOption("", string(b))
	}
}

func (p *tagParser) parseValue() {
	const quote = '\''

	c := p.Peek()
	if c == quote {
		p.Skip(quote)
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

func unquoteTagValue(s string) (string, bool) {
	const quote = '\''

	if len(s) < 2 {
		return s, false
	}
	if s[0] == quote && s[len(s)-1] == quote {
		return s[1 : len(s)-1], true
	}
	return s, false
}
