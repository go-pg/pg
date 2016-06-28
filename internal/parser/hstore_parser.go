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

	if !p.Skip('"') {
		return nil, fmt.Errorf("pg: can't parse hstore key: %q", p.Bytes())
	}

	key := p.readSubstring()
	if !(p.Skip('=') && p.Skip('>')) {
		return nil, fmt.Errorf("pg: can't parse hstore key: %q", p.Bytes())
	}

	return key, nil
}

func (p *HstoreParser) NextValue() ([]byte, error) {
	if !p.Skip('"') {
		return nil, fmt.Errorf("pg: can't parse hstore value: %q", p.Bytes())
	}

	value := p.readSubstring()
	p.SkipBytes([]byte(", "))
	return value, nil
}
