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
			if !v.IsNil() {
				v.Set(reflect.Zero(v.Type()))
			}
			return nil
		}

		table := GetTable(typ)
		p := parser.NewCompositeParser(b)
		for i := 0; p.Valid(); i++ {
			elem, err := p.NextElem()
			if err != nil {
				return err
			}

			field := table.Fields[i]
			err = field.ScanValue(v, elem)
			if err != nil {
				return err
			}
		}

		return nil
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
