package urlstruct

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/codemodus/kace"
	"github.com/go-pg/zerochecker"
	"github.com/vmihailenco/tagparser"
)

type OpCode int

const (
	OpEq OpCode = iota + 1
	OpNotEq
	OpLT
	OpLTE
	OpGT
	OpGTE
	OpIEq
	OpMatch
)

type Field struct {
	Type  reflect.Type
	Name  string
	Index []int
	Tag   *tagparser.Tag

	Column string
	Op     OpCode

	noDecode bool
	required bool
	noWhere  bool

	scanValue   scannerFunc
	isZeroValue zerochecker.Func
}

func (f *Field) init() {
	_, f.required = f.Tag.Options["required"]
	_, f.noDecode = f.Tag.Options["nodecode"]
	_, f.noWhere = f.Tag.Options["nowhere"]
	if f.required && f.noWhere {
		err := fmt.Errorf("urlstruct: required and nowhere tags can't be set together")
		panic(err)
	}

	const sep = "_"
	f.Column, f.Op = splitColumnOperator(kace.Snake(f.Name), sep)

	if f.Type.Kind() == reflect.Slice {
		f.scanValue = sliceScanner(f.Type)
	} else {
		f.scanValue = scanner(f.Type)
	}
	f.isZeroValue = zerochecker.Checker(f.Type)
}

func (f *Field) Value(strct reflect.Value) reflect.Value {
	return strct.FieldByIndex(f.Index)
}

func (f *Field) Omit(value reflect.Value) bool {
	return !f.required && (f.noWhere || f.isZeroValue(value))
}

func splitColumnOperator(s, sep string) (string, OpCode) {
	ind := strings.LastIndex(s, sep)
	if ind == -1 {
		return s, OpEq
	}

	col := s[:ind]
	op := s[ind+len(sep):]

	switch op {
	case "eq", "":
		return col, OpEq
	case "neq", "exclude":
		return col, OpNotEq
	case "gt":
		return col, OpGT
	case "gte":
		return col, OpGTE
	case "lt":
		return col, OpLT
	case "lte":
		return col, OpLTE
	case "ieq":
		return col, OpIEq
	case "match":
		return col, OpMatch
	default:
		return s, OpEq
	}
}
