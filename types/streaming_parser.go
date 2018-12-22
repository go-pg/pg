package types

import "fmt"

type streamingParser struct {
	Reader
}

func newStreamingParser(rd Reader) streamingParser {
	return streamingParser{
		Reader: rd,
	}
}

func (p streamingParser) SkipByte(skip byte) error {
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

func (p streamingParser) ReadSubstring() ([]byte, error) {
	var b []byte
	for {
		c, err := p.ReadByte()
		if err != nil {
			return b, err
		}

		switch c {
		case '\\':
			c2, err := p.ReadByte()
			if err != nil {
				return b, err
			}

			switch c2 {
			case '\\':
				b = append(b, '\\')
			case '"':
				b = append(b, '"')
			default:
				b = append(b, '\\')
				err = p.UnreadByte()
				if err != nil {
					return b, err
				}
			}
		case '\'':
			c2, err := p.ReadByte()
			if err != nil {
				return b, err
			}

			if c2 == '\'' {
				b = append(b, '\'')
			} else {
				b = append(b, c)
				err = p.UnreadByte()
				if err != nil {
					return b, err
				}
			}
		case '"':
			return b, nil
		default:
			b = append(b, c)
		}
	}
}
