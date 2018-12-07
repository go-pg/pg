package struct_filter

import (
	"reflect"
	"strings"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/internal/iszero"
	"github.com/go-pg/pg/internal/tag"
	"github.com/go-pg/pg/types"
)

type opCode int

const (
	opCodeEq opCode = iota + 1
	opCodeNotEq
	opCodeLT
	opCodeLTE
	opCodeGT
	opCodeGTE
)

var (
	opEq    = " = "
	opNotEq = " != "
	opLT    = " < "
	opLTE   = " <= "
	opGT    = " > "
	opGTE   = " >= "
	opAny   = " = ANY"
	opAll   = " != ALL"
)

type Field struct {
	index  []int
	column string

	opCode  opCode
	opValue string

	isSlice  bool
	ReadOnly bool
	omit     bool

	scan   ScanFunc
	append types.AppenderFunc
	isZero iszero.Func
}

func newField(sf reflect.StructField) *Field {
	f := &Field{
		index:   sf.Index,
		isSlice: sf.Type.Kind() == reflect.Slice,
	}

	pgTag := tag.Parse(sf.Tag.Get("pg"))
	if pgTag.Name == "-" {
		return nil
	}
	_, f.omit = pgTag.Options["omit"]
	_, f.ReadOnly = pgTag.Options["readonly"]

	if f.isSlice {
		f.column, f.opCode, f.opValue = splitSliceColumnOperator(sf.Name)
		f.scan = arrayScanner(sf.Type)
		f.append = types.ArrayAppender(sf.Type)
	} else {
		f.column, f.opCode, f.opValue = splitColumnOperator(sf.Name, "_")
		f.scan = scanner(sf.Type)
		f.append = types.Appender(sf.Type)
	}
	f.isZero = iszero.Checker(sf.Type)

	if f.scan == nil || f.append == nil {
		return nil
	}

	return f
}

func (f *Field) Value(strct reflect.Value) reflect.Value {
	return strct.FieldByIndex(f.index)
}

func (f *Field) Omit(value reflect.Value) bool {
	return f.omit || f.isZero(value)
}

func (f *Field) Scan(value reflect.Value, values []string) error {
	return f.scan(value, values)
}

func (f *Field) Append(b []byte, value reflect.Value) []byte {
	b = append(b, f.column...)
	b = append(b, f.opValue...)
	if f.isSlice {
		b = append(b, '(')
	}
	b = f.append(b, value, 1)
	if f.isSlice {
		b = append(b, ')')
	}
	return b
}

func splitColumnOperator(s, sep string) (string, opCode, string) {
	s = internal.Underscore(s)
	ind := strings.LastIndex(s, sep)
	if ind == -1 {
		return s, opCodeEq, opEq
	}

	col := s[:ind]
	op := s[ind+len(sep):]

	switch op {
	case "eq", "":
		return col, opCodeEq, opEq
	case "neq", "exclude":
		return col, opCodeNotEq, opNotEq
	case "gt":
		return col, opCodeGT, opGT
	case "gte":
		return col, opCodeGTE, opGTE
	case "lt":
		return col, opCodeLT, opLT
	case "lte":
		return col, opCodeLTE, opLTE
	default:
		return s, opCodeEq, opEq
	}
}

func splitSliceColumnOperator(s string) (string, opCode, string) {
	s = internal.Underscore(s)
	ind := strings.LastIndexByte(s, '_')
	if ind == -1 {
		return s, opCodeEq, opAny
	}

	col := s[:ind]
	op := s[ind+1:]

	switch op {
	case "eq", "":
		return col, opCodeEq, opAny
	case "neq", "exclude":
		return col, opCodeNotEq, opAll
	default:
		return s, opCodeEq, opAny
	}
}
