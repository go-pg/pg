package types

import "github.com/go-pg/pg/internal/parser"

func AppendJSONB(b, jsonb []byte, quote int) []byte {
	if quote == 1 {
		b = append(b, '\'')
	}

	p := parser.New(jsonb)
	for p.Valid() {
		c := p.Read()
		switch c {
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

	if quote == 1 {
		b = append(b, '\'')
	}

	return b
}
