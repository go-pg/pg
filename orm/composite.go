package orm

import (
	"fmt"
	"reflect"

	"github.com/go-pg/pg/internal/parser"
	"github.com/go-pg/pg/types"
)

func compositeScanner(typ reflect.Type) types.ScannerFunc {
	return func(v reflect.Value, b []byte) error {
		if !v.CanSet() {
			return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
		}

		if b == nil {
			v.Set(reflect.Zero(v.Type()))
			return nil
		}

		table := GetTable(typ)
		p := parser.NewCompositeParser(b)

		var firstErr error
		for i := 0; p.Valid(); i++ {
			elem, err := p.NextElem()
			if err != nil {
				return err
			}

			if i >= len(table.allFields) {
				if firstErr == nil {
					firstErr = fmt.Errorf(
						"%s has %d fields, but composite at least %d values",
						table, len(table.allFields), i)
				}
				continue
			}

			field := table.allFields[i]
			err = field.ScanValue(v, elem)
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}

		return firstErr
	}
}

func compositeAppender(typ reflect.Type) types.AppenderFunc {
	return func(b []byte, v reflect.Value, quote int) []byte {
		table := GetTable(typ)
		b = append(b, '(')
		for i, f := range table.Fields {
			if i > 0 {
				b = append(b, ',')
			}
			b = f.AppendValue(b, v, quote)
		}
		b = append(b, ')')
		return b
	}
}
