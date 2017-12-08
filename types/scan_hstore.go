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
	return func(v reflect.Value, b []byte) error {
		return fmt.Errorf("pg.Hstore(unsupported %s)", v.Type())
	}
}

func scanMapStringString(b []byte) (map[string]string, error) {
	if b == nil {
		return nil, nil
	}

	p := parser.NewHstoreParser(b)
	m := make(map[string]string)
	for p.Valid() {
		key, err := p.NextKey()
		if err != nil {
			return nil, err
		}
		if key == nil {
			return nil, fmt.Errorf("pg: unexpected NULL: %q", b)
		}

		value, err := p.NextValue()
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, fmt.Errorf("pg: unexpected NULL: %q", b)
		}

		m[string(key)] = string(value)
	}
	return m, nil
}

func scanMapStringStringValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}
	m, err := scanMapStringString(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(m))
	return nil
}
