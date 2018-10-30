package types

type streamingParser struct {
	Reader
}

func newStreamingParser(rd Reader) streamingParser {
	return streamingParser{
		Reader: rd,
	}
}

func (p streamingParser) ReadSubstring() ([]byte, error) {
	var b []byte
	for {
		c, err := p.ReadByte()
		if err != nil {
			return nil, err
		}

		switch c {
		case '\\':
			c2, err := p.ReadByte()
			if err != nil {
				return nil, err
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
					return nil, err
				}
			}
		case '\'':
			c2, err := p.ReadByte()
			if err != nil {
				return nil, err
			}

			if c2 == '\'' {
				b = append(b, '\'')
			} else {
				b = append(b, c)
				err = p.UnreadByte()
				if err != nil {
					return nil, err
				}
			}
		case '"':
			return b, nil
		default:
			b = append(b, c)
		}
	}
}
