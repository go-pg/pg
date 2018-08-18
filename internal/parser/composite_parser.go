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
		b, err := p.ReadString()
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
