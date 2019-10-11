package types

import "github.com/go-pg/pg/v8/internal/parser"

func AppendJSONB(b, jsonb []byte, quote int) []byte {
	switch quote {
	case 1:
		b = append(b, '\'')
	case 2:
		b = append(b, '"')
	}

	p := parser.New(jsonb)
	for p.Valid() {
		c := p.Read()
		switch c {
		case '"':
			if quote == 2 {
				b = append(b, '\\')
			}
			b = append(b, '"')
		case '\'':
			if quote == 1 {
				b = append(b, '\'', '\'')
			} else {
				b = append(b, '\'')
			}
		case '\000':
			continue
		case '\\':
			if p.SkipBytes([]byte("u0000")) {
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

	switch quote {
	case 1:
		b = append(b, '\'')
	case 2:
		b = append(b, '"')
	}

	return b
}
