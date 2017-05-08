package types

import "github.com/go-pg/pg/internal/parser"

func AppendField(b []byte, field string, quote int) []byte {
	return appendField(b, parser.NewString(field), quote)
}

func AppendFieldBytes(b []byte, field []byte, quote int) []byte {
	return appendField(b, parser.New(field), quote)
}

func appendField(b []byte, p *parser.Parser, quote int) []byte {
	var quoted bool
	for p.Valid() {
		c := p.Read()
		switch c {
		case '*':
			if !quoted {
				b = append(b, '*')
				continue
			}
		case '.':
			if quoted && quote == 1 {
				b = append(b, '"')
				quoted = false
			}
			b = append(b, '.')
			if p.Skip('*') {
				b = append(b, '*')
			} else if quote == 1 {
				b = append(b, '"')
				quoted = true
			}
			continue
		}

		if !quoted && quote == 1 {
			b = append(b, '"')
			quoted = true
		}
		if c == '"' {
			b = append(b, '"', '"')
		} else {
			b = append(b, c)
		}
	}
	if quoted && quote == 1 {
		b = append(b, '"')
	}
	return b
}
