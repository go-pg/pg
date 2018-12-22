package types

import (
	"fmt"
	"reflect"
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

	p := newHstoreParser(rd)
	m := make(map[string]string)
	for {
		key, err := p.NextKey()
		if err != nil {
			if err == endOfHstore {
				break
			}
			return nil, err
		}

		value, err := p.NextValue()
		if err != nil {
			return nil, err
		}

		m[string(key)] = string(value)
	}
	return m, nil
}
