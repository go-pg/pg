package types

import "gopkg.in/pg.v4/internal/parser"

func AppendJSONB(b, jsonb []byte, quote bool) []byte {
	if quote {
		b = append(b, '\'')
	}

	p := parser.New(jsonb)
	for p.Valid() {
		c := p.Read()
		switch c {
		case '\'':
			if quote {
				b = append(b, '\'', '\'')
			} else {
				b = append(b, '\'')
			}
		case '\000':
			continue
		case '\\':
			if p.Got("u0000") {
				b = append(b, "\\\\u0000"...)
			} else {
				b = append(b, '\\')
				if p.Valid() {
					b = append(b, p.Read())
				}
			}
		default:
			b = append(b, c)
		}
	}

	if quote {
		b = append(b, '\'')
	}

	return b
}
