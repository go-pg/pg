package parser

import "fmt"

type HstoreParser struct {
	*Parser
}

func NewHstoreParser(b []byte) *HstoreParser {
	return &HstoreParser{
		Parser: New(b),
	}
}

func (p *HstoreParser) NextKey() ([]byte, error) {
	if p.Skip(',') {
		p.Skip(' ')
	}

	key, err := p.ReadSubstring()
	if err != nil {
		return nil, err
	}

	if !(p.Skip('=') && p.Skip('>')) {
		return nil, fmt.Errorf("pg: can't parse hstore key: %q", p.Bytes())
	}

	return key, nil
}

func (p *HstoreParser) NextValue() ([]byte, error) {
	value, err := p.ReadSubstring()
	if err != nil {
		return nil, err
	}

	p.SkipBytes([]byte(", "))
	return value, nil
}
