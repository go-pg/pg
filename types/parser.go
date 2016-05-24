package types

import (
	"bytes"
	"fmt"

	"gopkg.in/pg.v4/internal/parser"
)

var pgNull = []byte("NULL")

type hstoreParser struct {
	*parser.Parser
}

func newHstoreParser(b []byte) *hstoreParser {
	return &hstoreParser{
		Parser: parser.New(b),
	}
}

func (p *hstoreParser) NextKey() ([]byte, error) {
	if p.Got(",") {
		p.Got(" ")
	}

	if !p.Got(`"`) {
		return nil, fmt.Errorf("pg: can't parse hstore key: %q", p.Bytes())
	}

	b := make([]byte, 0)
	for p.Valid() {
		c := p.Read()
		switch c {
		case '\\':
			switch p.Peek() {
			case '\\':
				b = append(b, '\\')
				p.Skip(c)
			case '"':
				b = append(b, '"')
				p.Skip(c)
			default:
				b = append(b, c)
			}
		case '\'':
			switch p.Peek() {
			case '\'':
				b = append(b, '\'')
				p.Skip(c)
			default:
				b = append(b, c)
			}
		case '"':
			// Read "=>".
			p.Skip('=')
			p.Skip('>')
			return b, nil
		default:
			b = append(b, c)
		}
	}

	return nil, fmt.Errorf("pg: can't parse hstore: %q", p.Bytes())
}

func (p *hstoreParser) NextValue() ([]byte, error) {
	if !p.Got(`"`) {
		bb, _ := p.ReadSep(',')
		p.Skip(' ')
		if bytes.Equal(bb, pgNull) {
			return nil, nil
		}
		return bb, nil
	}

	b := make([]byte, 0)
	for p.Valid() {
		c := p.Read()
		switch c {
		case '\\':
			switch p.Peek() {
			case '\\':
				b = append(b, '\\')
				p.Skip(c)
			case '"':
				b = append(b, '"')
				p.Skip(c)
			default:
				b = append(b, c)
			}
		case '\'':
			switch p.Peek() {
			case '\'':
				b = append(b, '\'')
				p.Skip(c)
			default:
				b = append(b, c)
			}
		case '"':
			return b, nil
		default:
			b = append(b, c)
		}
	}

	return nil, fmt.Errorf("pg: can't parse hstore: %s", p.Bytes())
}
