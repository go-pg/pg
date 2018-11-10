package types

import (
	"fmt"
	"reflect"

	"github.com/go-pg/pg/internal/parser"
)

func HstoreScanner(typ reflect.Type) ScannerFunc {
	if typ.Key() == stringType && typ.Elem() == stringType {
		return scanMapStringStringValue
	}
	return func(v reflect.Value, rd Reader, n int) error {
		return fmt.Errorf("pg.Hstore(unsupported %s)", v.Type())
	}
}

func scanMapStringStringValue(v reflect.Value, rd Reader, n int) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}

	m, err := scanMapStringString(rd, n)
	if err != nil {
		return err
	}

	v.Set(reflect.ValueOf(m))
	return nil
}

func scanMapStringString(rd Reader, n int) (map[string]string, error) {
	if n == -1 {
		return nil, nil
	}

	tmp, err := rd.ReadFullTemp()
	if err != nil {
		return nil, err
	}

	p := parser.NewHstoreParser(tmp)
	m := make(map[string]string)
	for p.Valid() {
		key, err := p.NextKey()
		if err != nil {
			return nil, err
		}
		if key == nil {
			return nil, fmt.Errorf("pg: unexpected NULL: %q", tmp)
		}

		value, err := p.NextValue()
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, fmt.Errorf("pg: unexpected NULL: %q", tmp)
		}

		m[string(key)] = string(value)
	}
	return m, nil
}
