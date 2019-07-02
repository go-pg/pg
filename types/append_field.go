package types

import "github.com/go-pg/pg/v9/internal"

func AppendField(b []byte, field string, flags int) []byte {
	return appendField(b, internal.StringToBytes(field), flags)
}

func AppendFieldBytes(b []byte, field []byte, flags int) []byte {
	return appendField(b, field, flags)
}

func appendField(b, src []byte, flags int) []byte {
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
			if quoted && hasFlag(flags, quoteFlag) {
				b = append(b, '"')
				quoted = false
			}
			b = append(b, '.')
			continue loop
		}

		if !quoted && hasFlag(flags, quoteFlag) {
			b = append(b, '"')
			quoted = true
		}
		if c == '"' {
			b = append(b, '"', '"')
		} else {
			b = append(b, c)
		}
	}
	if quoted && hasFlag(flags, quoteFlag) {
		b = append(b, '"')
	}
	return b
}
