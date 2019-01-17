package orm

import (
	"fmt"
	"reflect"

	"github.com/go-pg/pg/types"
)

func compositeScanner(typ reflect.Type) types.ScannerFunc {
	return func(v reflect.Value, rd types.Reader, n int) error {
		if !v.CanSet() {
			return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
		}

		if n == -1 {
			v.Set(reflect.Zero(v.Type()))
			return nil
		}

		table := GetTable(typ)
		p := newCompositeParser(rd)
		var elemReader *types.BytesReader

		var firstErr error
		for i := 0; ; i++ {
			elem, err := p.NextElem()
			if err != nil {
				if err == endOfComposite {
					break
				}
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

			if elemReader == nil {
				elemReader = types.NewBytesReader(elem)
			} else {
				elemReader.Reset(elem)
			}

			field := table.allFields[i]
			err = field.ScanValue(v, elemReader, len(elem))
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}

		return firstErr
	}
}

func compositeAppender(typ reflect.Type) types.AppenderFunc {
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
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
