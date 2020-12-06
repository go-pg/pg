package types

import (
	"bytes"
	"io"
	"unicode"

	"github.com/pkg/errors"
)

// -----------------------------------------------------------------------------------------------------
// The code in this file is taken from github.com/jackc/pgtype and was slightly modified.
// Modifications were aimed at only taking the code necessary to fulfill the tstzrange requirements.
// -----------------------------------------------------------------------------------------------------

type UntypedTextRange struct {
	Lower     string
	Upper     string
	LowerType BoundType
	UpperType BoundType
}

func parseUntypedTextRange(src string) (*UntypedTextRange, error) {
	utr := &UntypedTextRange{}
	if src == "empty" {
		utr.LowerType = RBoundEmpty
		utr.UpperType = RBoundEmpty
		return utr, nil
	}

	buf := bytes.NewBufferString(src)

	skipWhitespace(buf)

	r, _, err := buf.ReadRune()
	if err != nil {
		return nil, errors.Errorf("invalid Lower bound: %v", err)
	}
	switch r {
	case '(':
		utr.LowerType = RBoundExclusive
	case '[':
		utr.LowerType = RBoundInclusive
	default:
		return nil, errors.Errorf("missing Lower bound, instead got: %v", string(r))
	}

	r, _, err = buf.ReadRune()
	if err != nil {
		return nil, errors.Errorf("invalid Lower value: %v", err)
	}
	_ = buf.UnreadRune()

	if r == ',' {
		utr.LowerType = RBoundUnbounded
	} else {
		utr.Lower, err = rangeParseValue(buf)
		if err != nil {
			return nil, errors.Errorf("invalid Lower value: %v", err)
		}
	}

	r, _, err = buf.ReadRune()
	if err != nil {
		return nil, errors.Errorf("missing range separator: %v", err)
	}
	if r != ',' {
		return nil, errors.Errorf("missing range separator: %v", r)
	}

	r, _, err = buf.ReadRune()
	if err != nil {
		return nil, errors.Errorf("invalid Upper value: %v", err)
	}

	if r == ')' || r == ']' {
		utr.UpperType = RBoundUnbounded
	} else {
		_ = buf.UnreadRune()

		utr.Upper, err = rangeParseValue(buf)
		if err != nil {
			return nil, errors.Errorf("invalid Upper value: %v", err)
		}

		r, _, err = buf.ReadRune()
		if err != nil {
			return nil, errors.Errorf("missing Upper bound: %v", err)
		}
		switch r {
		case ')':
			utr.UpperType = RBoundExclusive
		case ']':
			utr.UpperType = RBoundInclusive
		default:
			return nil, errors.Errorf("missing Upper bound, instead got: %v", string(r))
		}
	}

	skipWhitespace(buf)

	if buf.Len() > 0 {
		return nil, errors.Errorf("unexpected trailing data: %v", buf.String())
	}

	return utr, nil
}

// -----------------------------------------------------------------------------------------------------

func rangeParseValue(buf io.RuneScanner) (string, error) {
	r, _, err := buf.ReadRune()
	if err != nil {
		return "", err
	}
	if r == '"' {
		return rangeParseQuotedValue(buf)
	}
	_ = buf.UnreadRune()

	s := &bytes.Buffer{}

	for {
		r, _, err := buf.ReadRune()
		if err != nil {
			return "", err
		}

		switch r {
		case '\\':
			r, _, err = buf.ReadRune()
			if err != nil {
				return "", err
			}
		case ',', '[', ']', '(', ')':
			_ = buf.UnreadRune()
			return s.String(), nil
		}

		s.WriteRune(r)
	}
}

func rangeParseQuotedValue(buf io.RuneScanner) (string, error) {
	s := &bytes.Buffer{}

	for {
		r, _, err := buf.ReadRune()
		if err != nil {
			return "", err
		}

		switch r {
		case '\\':
			r, _, err = buf.ReadRune()
			if err != nil {
				return "", err
			}
		case '"':
			r, _, err = buf.ReadRune()
			if err != nil {
				return "", err
			}
			if r != '"' {
				_ = buf.UnreadRune()
				return s.String(), nil
			}
		}
		s.WriteRune(r)
	}
}

func skipWhitespace(buf io.RuneScanner) {
	var r rune
	var err error
	for r, _, _ = buf.ReadRune(); unicode.IsSpace(r); r, _, _ = buf.ReadRune() {
	}

	if err != io.EOF {
		_ = buf.UnreadRune()
	}
}
