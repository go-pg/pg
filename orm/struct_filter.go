package orm

import (
	"reflect"
	"sync"

	"github.com/go-pg/pg/v9/types"
	"github.com/go-pg/urlstruct"
)

var ops = [...]string{
	urlstruct.OpEq:    " = ",
	urlstruct.OpNotEq: " != ",
	urlstruct.OpLT:    " < ",
	urlstruct.OpLTE:   " <= ",
	urlstruct.OpGT:    " > ",
	urlstruct.OpGTE:   " >= ",
	urlstruct.OpIEq:   " ILIKE ",
	urlstruct.OpMatch: " SIMILAR TO ",
}

var sliceOps = [...]string{
	urlstruct.OpEq:    " = ANY",
	urlstruct.OpNotEq: " != ALL",
}

func getOp(ops []string, op urlstruct.OpCode) string {
	if int(op) < len(ops) {
		return ops[op]
	}
	return ""
}

type structFilter struct {
	value reflect.Value // reflect.Struct

	infoOnce sync.Once
	info     *urlstruct.StructInfo // lazy
}

var _ queryWithSepAppender = (*structFilter)(nil)

func newStructFilter(v interface{}) *structFilter {
	if v, ok := v.(*structFilter); ok {
		return v
	}
	return &structFilter{
		value: reflect.Indirect(reflect.ValueOf(v)),
	}
}

func (sf *structFilter) AppendSep(b []byte) []byte {
	return append(b, " AND "...)
}

func (sf *structFilter) AppendQuery(fmter QueryFormatter, b []byte) ([]byte, error) {
	sf.infoOnce.Do(func() {
		sf.info = urlstruct.DescribeStruct(sf.value.Type())
	})

	isPlaceholder := isPlaceholderFormatter(fmter)
	startLen := len(b)

	prevLen := len(b)
	for _, f := range sf.info.Fields {
		fv := f.Value(sf.value)
		if f.Omit(fv) {
			continue
		}

		isSlice := f.Type.Kind() == reflect.Slice

		var op string
		if isSlice {
			op = getOp(sliceOps[:], f.Op)
		} else {
			op = getOp(ops[:], f.Op)
		}
		if op == "" {
			continue
		}

		var appendValue types.AppenderFunc
		if isSlice {
			appendValue = types.ArrayAppender(f.Type)
		} else {
			appendValue = types.Appender(f.Type)
		}
		if appendValue == nil {
			continue
		}

		if len(b) != prevLen {
			b = append(b, " AND "...)
			prevLen = len(b)
		}

		if sf.info.TableName != "" {
			b = types.AppendIdent(b, sf.info.TableName, 1)
			b = append(b, '.')
		}
		b = append(b, f.Column...)
		b = append(b, op...)
		if isSlice {
			b = append(b, '(')
		}
		if isPlaceholder {
			b = append(b, '?')
		} else {
			b = appendValue(b, fv, 1)
		}
		if isSlice {
			b = append(b, ')')
		}
	}

	if len(b) == startLen {
		b = append(b, "TRUE"...)
	}

	return b, nil
}
