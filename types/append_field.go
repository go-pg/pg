package types

import "github.com/go-pg/pg/internal"

func AppendField(b []byte, field string, quote int) []byte {
	return appendField(b, internal.StringToBytes(field), quote)
}

func AppendFieldBytes(b []byte, field []byte, quote int) []byte {
	return appendField(b, field, quote)
}

func appendField(b, src []byte, quote int) []byte {
	var quoted bool
loop:
	for _, c := range src {
		switch c {
		case '*':
			if !quoted {
				b = append(b, '*')
				continue loop
			}
		case '.':
			if quoted && quote == 1 {
				b = append(b, '"')
				quoted = false
			}
			b = append(b, '.')
			continue loop
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
