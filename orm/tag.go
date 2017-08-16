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
		if value == "" && p.tag.Name == "" {
			p.tag.Name = key
			return
		}
	}
	p.tag.Options[key] = value
}

func (p *tagParser) parseKey() {
	var b []byte
	for p.Valid() {
		c := p.Read()
		switch c {
		case ',':
			p.Skip(' ')
			p.setTagOption(string(b), "")
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
		p.setTagOption(string(b), "")
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
	if len(b) > 0 {
		p.setTagOption(p.key, string(b))
	}
}

func (p *tagParser) parseQuotedValue() {
	const quote = '\''

	if !p.Skip(quote) {
		panic("not reached")
	}

	var b []byte
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
		p.Skip(quote)

		p.setTagOption(p.key, string(b))
		p.parseKey()
		return
	}
	if len(b) > 0 {
		p.setTagOption(p.key, string(b))
	}
}
