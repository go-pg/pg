package types

/*
#include <locale.h>
*/
import "C"
import (
	"fmt"
	"strings"
	"unicode"
)

type Money float64

func (m *Money) Scan(b interface{}) error {
	if b == nil {
		*m = 0
		return nil
	}
	*m = Money(cashIn(string(b.([]byte))))
	return nil
}

func SetMonetaryLocale(locale string) {
	C.setlocale(3, C.CString(locale))
}

func pg_mul_s64_overflow(a int64, b int64, result *int64) (overflow bool) {
	*result = a * b
	return false
}

func pg_sub_s64_overflow(a int64, b int64, result *int64) (overflow bool) {
	*result = a - b
	return false
}

// This function is taken directly from PostgreSQL's source code.
// Path: src/backend/utils/adt/cash.c
func cashIn(input string) float64 {
	lconvert := C.localeconv()

	var result float64
	var value int64 = 0
	var dec int64 = 0
	var sgn = 1
	var seenDot = false
	var s = input
	var fpoint int64
	var dsymbol string
	var ssymbol string
	var csymbol string
	var psymbol string
	var nsymbol string

	/*
	 * frac_digits will be CHAR_MAX in some locales, notably C.  However, just
	 * testing for == CHAR_MAX is risky, because of compilers like gcc that
	 * "helpfully" let you alter the platform-standard definition of whether
	 * char is signed or not.  If we are so unfortunate as to get compiled
	 * with a nonstandard -fsigned-char or -funsigned-char switch, then our
	 * idea of CHAR_MAX will not agree with libc's. The safest course is not
	 * to test for CHAR_MAX at all, but to impose a range check for plausible
	 * frac_digits values.
	 */
	fpoint = int64(lconvert.frac_digits)
	if fpoint < 0 || fpoint > 10 {
		fpoint = 2 /* best guess in this case, I think */
	}

	mon_decimal_point := C.GoString(lconvert.mon_decimal_point)
	/* we restrict dsymbol to be a single byte, but not the other symbols */
	if mon_decimal_point != "" &&
		len(mon_decimal_point) > 0 {
		dsymbol = mon_decimal_point
	} else {
		dsymbol = "."
	}

	monThousandsSep := C.GoString(lconvert.mon_thousands_sep)
	if monThousandsSep != "" {
		ssymbol = monThousandsSep
	} else {
		if dsymbol != "," {
			ssymbol = ","
		} else {
			ssymbol = "."
		}
	}

	currencySymbol := C.GoString(lconvert.currency_symbol)
	positiveSign := C.GoString(lconvert.positive_sign)
	negativeSign := C.GoString(lconvert.negative_sign)

	if currencySymbol != "" {
		csymbol = currencySymbol
	} else {
		csymbol = "$"
	}

	if positiveSign != "" {
		psymbol = positiveSign
	} else {
		psymbol = "+"
	}

	if negativeSign != "" {
		nsymbol = negativeSign
	} else {
		nsymbol = "-"
	}

	/* we need to add all sorts of checking here.  For now just */
	/* strip all leading whitespace and any leading currency symbol */
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, csymbol)
	s = strings.TrimSpace(s)

	/* a leading minus or paren signifies a negative number */
	/* again, better heuristics needed */
	/* XXX - doesn't properly check for balanced parens - djmc */
	if strings.HasPrefix(s, nsymbol) {
		sgn = -1
		s = strings.TrimPrefix(s, nsymbol)
	} else if strings.HasPrefix(s, "(") {
		sgn = -1
		s = strings.TrimPrefix(s, "(")
	} else if strings.HasPrefix(s, psymbol) {
		s = strings.TrimPrefix(s, psymbol)
	}

	/* allow whitespace and currency symbol after the sign, too */
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, csymbol)
	s = strings.TrimSpace(s)

	/*
	 * We accumulate the absolute amount in "value" and then apply the sign at
	 * the end.  (The sign can appear before or after the digits, so it would
	 * be more complicated to do otherwise.)  Because of the larger range of
	 * negative signed integers, we build "value" in the negative and then
	 * flip the sign at the end, catching most-negative-number overflow if
	 * necessary.
	 */
	for ; len(s) > 0; s = s[1:] {
		c := rune(s[0])
		/*
		 * We look for digits as long as we have found less than the required
		 * number of decimal places.
		 */
		if unicode.IsDigit(c) && (!seenDot || dec < fpoint) {
			digit := c - '0'

			if pg_mul_s64_overflow(value, 10, &value) ||
				pg_sub_s64_overflow(value, int64(int8(digit)), &value) {
				panic(fmt.Sprintf(`value "%s" is out of range for type %s`, input, "money"))
			}

			if seenDot {
				dec++
			}
		} else if strings.HasPrefix(s, dsymbol) && !seenDot {
			seenDot = true
		} else if strings.HasPrefix(s, ssymbol) {
			s = s[len(ssymbol)-1:]
		} else {
			break
		}
	}

	// ADD ROUNDING HERE
	if len(s) > 0 && unicode.IsDigit(rune(s[0])) && rune(s[0]) > '5' {
		if pg_sub_s64_overflow(value, 1, &value) {
			panic(fmt.Sprintf(`value "%s" is out of range for type %s`, input, "money"))
		}
	}

	/* adjust for less than required decimal places */
	for ; dec < fpoint; dec++ {
		if pg_mul_s64_overflow(value, 10, &value) {
			panic(fmt.Sprintf(`value "%s" is out of range for type %s`, input, "money"))
		}
	}

	/*
	 * should only be trailing digits followed by whitespace, right paren,
	 * trailing sign, and/or trailing currency symbol
	 */
	for ; len(s) > 0 && unicode.IsDigit(rune(s[0])); s = s[1:] {
	}

	for len(s) > 0 {
		if unicode.IsSpace(rune(s[0])) || rune(s[0]) == ')' {
			s = s[1:]
		} else if strings.HasPrefix(s, nsymbol) {
			sgn = -1
			s = strings.TrimPrefix(s, nsymbol)
		} else if strings.HasPrefix(s, psymbol) {
			s = strings.TrimPrefix(s, csymbol)
		} else if strings.HasPrefix(s, csymbol) {
			s = strings.TrimPrefix(s, csymbol)
		} else {
			panic(fmt.Sprintf(`invalid input syntax for type %s: "%s"`, "money", input))
		}
	}

	if sgn > 0 {
		if value == -0x7FFFFFFFFFFFFFFF {
			panic(fmt.Sprintf(`value "%s" is out of range for type %s`, input, "money"))
		}
		result = float64(-value)
	} else {
		result = float64(value)
	}

	return result / 100
}
