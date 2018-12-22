package types

import (
	"errors"
	"io"
)

var endOfHstore = errors.New("pg: end of hstore")

type hstoreParser struct {
	p streamingParser
}

func newHstoreParser(rd Reader) *hstoreParser {
	return &hstoreParser{
		p: newStreamingParser(rd),
	}
}

func (p *hstoreParser) NextKey() ([]byte, error) {
	err := p.p.SkipByte('"')
	if err != nil {
		if err == io.EOF {
			return nil, endOfHstore
		}
		return nil, err
	}

	key, err := p.p.ReadSubstring()
	if err != nil {
		return nil, err
	}

	err = p.p.SkipByte('=')
	if err != nil {
		return nil, err
	}
	err = p.p.SkipByte('>')
	if err != nil {
		return nil, err
	}

	return key, nil
}

func (p *hstoreParser) NextValue() ([]byte, error) {
	err := p.p.SkipByte('"')
	if err != nil {
		return nil, err
	}

	value, err := p.p.ReadSubstring()
	if err != nil {
		return nil, err
	}

	err = p.p.SkipByte(',')
	if err == nil {
		_ = p.p.SkipByte(' ')
	}

	return value, nil
}
