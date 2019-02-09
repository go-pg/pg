package struct_filter

import (
	"reflect"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/internal/tag"
)

type Struct struct {
	TableName string
	Fields    []*Field
}

func NewStruct(typ reflect.Type) *Struct {
	s := &Struct{
		Fields: make([]*Field, 0, typ.NumField()),
	}
	addFields(s, typ, nil)
	return s
}

func (s *Struct) Field(name string) *Field {
	col, opCode, _ := splitColumnOperator(name, "__")
	for _, f := range s.Fields {
		if f.Column == col && f.opCode == opCode {
			return f
		}
	}
	return nil
}

func addFields(s *Struct, typ reflect.Type, baseIndex []int) {
	if baseIndex != nil {
		baseIndex = baseIndex[:len(baseIndex):len(baseIndex)]
	}
	for i := 0; i < typ.NumField(); i++ {
		sf := typ.Field(i)
		if sf.Anonymous {
			pgTag := sf.Tag.Get("pg")
			if pgTag == "-" {
				continue
			}

			sfType := sf.Type
			if sfType.Kind() == reflect.Ptr {
				sfType = sfType.Elem()
			}
			if sfType.Kind() != reflect.Struct {
				continue
			}

			addFields(s, sfType, sf.Index)
			continue
		}

		if sf.Name == "tableName" {
			sqlTag := tag.Parse(sf.Tag.Get("sql"))
			name, _ := tag.Unquote(sqlTag.Name)
			s.TableName = internal.QuoteTableName(name)
			continue
		}

		f := newField(sf)
		if f == nil {
			continue
		}
		if len(baseIndex) > 0 {
			f.index = append(baseIndex, f.index...)
		}
		s.Fields = append(s.Fields, f)
	}

}
