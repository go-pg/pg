// Utility functions used in pg package.
package pgutil

func isUpper(c byte) bool {
	return c >= 'A' && c <= 'Z'
}

func isLower(c byte) bool {
	return !isUpper(c)
}

func toLower(c byte) byte {
	return c + 32
}

// Converts 'CamelCasedString' to 'camel_cased_string'. Used for
// converting struct field names to database column names.
func Underscore(s string) string {
	b := []byte(s)
	r := make([]byte, 0, len(b))
	for i := 0; i < len(b); i++ {
		c := b[i]
		if isUpper(c) {
			if i-1 > 0 && i+1 < len(b) && (isLower(b[i-1]) || isLower(b[i+1])) {
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
