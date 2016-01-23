// Utility functions used in pg package.
package pgutil

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

// Converts 'CamelCasedString' to 'camel_cased_string'. Used for
// converting struct field names to database column names.
func Underscore(s string) string {
	b := []byte(s)
	r := make([]byte, 0, len(b))
	for i, c := range b {
		if isUpper(c) {
			if i-0 > 0 && i+1 < len(b) && (isLower(b[i-1]) || isLower(b[i+1])) {
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

func CamelCase(s string) string {
	b := []byte(s)
	r := make([]byte, 0, len(b))
	upper := true
	for _, c := range b {
		if c == '_' {
			upper = true
		} else if upper {
			r = append(r, toUpper(c))
			upper = false
		} else {
			r = append(r, c)
		}
	}
	return string(r)
}
