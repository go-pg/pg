package parser

import (
	"fmt"

	"github.com/go-pg/pg/internal"
)

type StreamingParser struct {
	internal.Reader
}

func NewStreamingParser(rd internal.Reader) StreamingParser {
	return StreamingParser{
		Reader: rd,
	}
}

func (p StreamingParser) SkipByte(skip byte) error {
	c, err := p.ReadByte()
	if err != nil {
		return err
	}
	if c == skip {
		return nil
	}
	_ = p.UnreadByte()
	return fmt.Errorf("got %q, wanted %q", c, skip)
}

func (p StreamingParser) ReadSubstring() ([]byte, error) {
	var b []byte

	c, err := p.ReadByte()
	if err != nil {
		return b, err
	}

	for {
		if c == '"' {
			return b, nil
		}

		next, err := p.ReadByte()
		if err != nil {
			return b, err
		}

		if c == '\\' {
			switch next {
			case '\\', '"':
				b = append(b, next)
				c, err = p.ReadByte()
				if err != nil {
					return nil, err
				}
			default:
				b = append(b, '\\')
				c = next
			}
			continue
		}

		if c == '\'' {
			if next == '\'' {
				b = append(b, '\'')
				c, err = p.ReadByte()
				if err != nil {
					return nil, err
				}
			} else {
				b = append(b, c)
				c = next
			}
			continue
		}

		b = append(b, c)
		c = next
	}
}
