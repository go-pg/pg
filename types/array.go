package types

import (
	"database/sql"
	"fmt"
	"strconv"
)

type Array struct {
	Value interface{}
}

var _ ValueAppender = (*Array)(nil)
var _ sql.Scanner = (*Array)(nil)

func (a Array) AppendValue(b []byte, quote bool) ([]byte, error) {
	switch v := a.Value.(type) {
	case []string:
		return appendStringSlice(b, v, quote), nil
	case []int:
		return appendIntSlice(b, v, quote), nil
	case []int64:
		return appendInt64Slice(b, v, quote), nil
	case []float64:
		return appendFloat64Slice(b, v, quote), nil
	}
	return nil, fmt.Errorf("pg: Append(%T)", a.Value)
}

func (a *Array) Scan(bi interface{}) error {
	b := bi.([]byte)
	var err error
	switch a.Value.(type) {
	case []string:
		a.Value, err = decodeStringSlice(b)
		return err
	case []int:
		a.Value, err = decodeIntSlice(b)
		return err
	case []int64:
		a.Value, err = decodeInt64Slice(b)
		return err
	case []float64:
		a.Value, err = decodeFloat64Slice(b)
		return err
	}
	return fmt.Errorf("pg: Decode(%T)", a.Value)
}

func appendStringSlice(b []byte, ss []string, quote bool) []byte {
	if ss == nil {
		return AppendNull(b, quote)
	}

	if quote {
		b = append(b, '\'')
	}

	b = append(b, '{')
	for _, s := range ss {
		b = appendSubstring(b, s, quote)
		b = append(b, ',')
	}
	if len(ss) > 0 {
		b[len(b)-1] = '}' // Replace trailing comma.
	} else {
		b = append(b, '}')
	}

	if quote {
		b = append(b, '\'')
	}

	return b
}

func appendIntSlice(b []byte, ints []int, quote bool) []byte {
	if ints == nil {
		return AppendNull(b, quote)
	}

	if quote {
		b = append(b, '\'')
	}

	b = append(b, '{')
	for _, n := range ints {
		b = strconv.AppendInt(b, int64(n), 10)
		b = append(b, ',')
	}
	if len(ints) > 0 {
		b[len(b)-1] = '}' // Replace trailing comma.
	} else {
		b = append(b, '}')
	}

	if quote {
		b = append(b, '\'')
	}

	return b
}

func appendInt64Slice(b []byte, ints []int64, quote bool) []byte {
	if ints == nil {
		return AppendNull(b, quote)
	}

	if quote {
		b = append(b, '\'')
	}

	b = append(b, "{"...)
	for _, n := range ints {
		b = strconv.AppendInt(b, n, 10)
		b = append(b, ',')
	}
	if len(ints) > 0 {
		b[len(b)-1] = '}' // Replace trailing comma.
	} else {
		b = append(b, '}')
	}

	if quote {
		b = append(b, '\'')
	}

	return b
}

func appendFloat64Slice(b []byte, floats []float64, quote bool) []byte {
	if floats == nil {
		return AppendNull(b, quote)
	}

	if quote {
		b = append(b, '\'')
	}

	b = append(b, "{"...)
	for _, n := range floats {
		b = appendFloat(b, n)
		b = append(b, ',')
	}
	if len(floats) > 0 {
		b[len(b)-1] = '}' // Replace trailing comma.
	} else {
		b = append(b, '}')
	}

	if quote {
		b = append(b, '\'')
	}

	return b
}

func decodeStringSlice(b []byte) ([]string, error) {
	p := newArrayParser(b)
	s := make([]string, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			return nil, err
		}
		if elem == nil {
			return nil, fmt.Errorf("pg: unexpected NULL: %q", b)
		}
		s = append(s, string(elem))
	}
	return s, nil
}

func decodeIntSlice(b []byte) ([]int, error) {
	p := newArrayParser(b)
	s := make([]int, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			return nil, err
		}
		if elem == nil {
			return nil, fmt.Errorf("pg: unexpected NULL: %q", b)
		}
		n, err := strconv.Atoi(string(elem))
		if err != nil {
			return nil, err
		}
		s = append(s, n)
	}
	return s, nil
}

func decodeInt64Slice(b []byte) ([]int64, error) {
	p := newArrayParser(b)
	s := make([]int64, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			return nil, err
		}
		if elem == nil {
			return nil, fmt.Errorf("pg: unexpected NULL: %q", b)
		}
		n, err := strconv.ParseInt(string(elem), 10, 64)
		if err != nil {
			return nil, err
		}
		s = append(s, n)
	}
	return s, nil
}

func decodeFloat64Slice(b []byte) ([]float64, error) {
	p := newArrayParser(b)
	slice := make([]float64, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			return nil, err
		}
		if elem == nil {
			return nil, fmt.Errorf("pg: unexpected NULL: %q", b)
		}
		n, err := strconv.ParseFloat(string(elem), 64)
		if err != nil {
			return nil, err
		}
		slice = append(slice, n)
	}
	return slice, nil
}
