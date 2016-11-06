package internal

func isUpper(c byte) bool {
	return c >= 'A' && c <= 'Z'
}

func isLower(c byte) bool {
	return !isUpper(c)
}

func toUpper(c byte) byte {
	return c - 32
}

func toLower(c byte) byte {
	return c + 32
}

// Underscore converts "CamelCasedString" to "camel_cased_string".
func Underscore(s string) string {
	r := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if isUpper(c) {
			if i > 0 && i+1 < len(s) && (isLower(s[i-1]) || isLower(s[i+1])) {
				r = append(r, '_', toLower(c))
			} else {
				r = append(r, toLower(c))
			}
		} else {
			r = append(r, c)
		}
	}
	return string(r)
}

func ToUpper(s string) string {
	if isUpperString(s) {
		return s
	}

	b := make([]byte, len(s))
	for i := range b {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

func isUpperString(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			return false
		}
	}
	return true
}

func ToExported(s string) string {
	if len(s) == 0 {
		return s
	}
	if c := s[0]; isLower(c) {
		b := []byte(s)
		b[0] = toUpper(c)
		return string(b)
	}
	return s
}
