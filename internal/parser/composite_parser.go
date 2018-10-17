package parser

import (
	"fmt"
)

type CompositeParser struct {
	*Parser

	stickyErr error
}

func NewCompositeParser(b []byte) *CompositeParser {
	var err error
	if len(b) < 2 || b[0] != '(' || b[len(b)-1] != ')' {
		err = fmt.Errorf("pg: can't parse composite value: %s", string(b))
	} else {
		b = b[1 : len(b)-1]
	}
	return &CompositeParser{
		Parser: New(b),

		stickyErr: err,
	}
}

func (p *CompositeParser) NextElem() ([]byte, error) {
	if p.stickyErr != nil {
		return nil, p.stickyErr
	}

	switch c := p.Peek(); c {
	case '"':
		b, err := p.readQuoted()
		if err != nil {
			return nil, err
		}

		if p.Valid() {
			if err := p.MustSkip(','); err != nil {
				return nil, err
			}
		}

		return b, nil
	default:
		b, _ := p.ReadSep(',')
		if len(b) == 0 { // NULL
			b = nil
		}
		return b, nil
	}
}

func (p *Parser) readQuoted() ([]byte, error) {
	if !p.Skip('"') {
		return nil, fmt.Errorf("pg: composite: can't find opening quote: %q", p.Bytes())
	}

	var b []byte
	for p.Valid() {
		c := p.Read()
		switch c {
		case '\\':
			switch p.Peek() {
			case '\\':
				p.Advance()
				b = append(b, '\\')
			default:
				b = append(b, c)
			}
		case '\'':
			if p.Peek() == '\'' {
				p.Advance()
				b = append(b, '\'')
			} else {
				b = append(b, c)
			}
		case '"':
			if p.Peek() == '"' {
				p.Advance()
				b = append(b, '"')
			} else {
				return b, nil
			}
		default:
			b = append(b, c)
		}
	}

	return nil, fmt.Errorf("pg: composite: can't find closing quote: %q", p.Bytes())
}
