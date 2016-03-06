package orm

import (
	"fmt"
	"strings"
)

type m2mModelFields struct {
	Prefix string
	Fields []*Field
}

type Relation struct {
	One         bool
	Polymorphic string

	Field *Field
	Join  *Table

	FKs []*Field

	M2M *Table

	M2MBaseFKs []*Field
	M2MJoinFKs []*Field

	m2mModelFields []m2mModelFields
}

func (rel *Relation) M2MModelFields(prefix string) m2mModelFields {
	prefix += "_"
	for _, m2m := range rel.m2mModelFields {
		if m2m.Prefix == prefix {
			return m2m
		}
	}
	return m2mModelFields{}
}

func (rel *Relation) addM2MModelFields(fields []*Field, prefix string) {
	prefix += "_"
	fields = fieldsWithPrefix(fields, prefix)
	if fields != nil {
		rel.m2mModelFields = append(rel.m2mModelFields, m2mModelFields{
			Prefix: prefix,
			Fields: fields,
		})
	}
}

func fieldsWithPrefix(fields []*Field, prefix string) []*Field {
	var dst []*Field
	for _, f := range fields {
		if !f.Has(ForeignKeyFlag) && strings.HasPrefix(f.SQLName, prefix) {
			dst = append(dst, f)
		}
	}
	return dst
}

func m2mFKs(t, m2m *Table) []*Field {
	fks := make([]*Field, 0, len(t.PKs))
	for _, pk := range t.PKs {
		fkName := t.ModelName + "_" + pk.SQLName
		fk := m2m.FieldsMap[fkName]
		if fk == nil {
			panic(fmt.Errorf("m2m base fk %q not found", fkName))
		}
		fks = append(fks, fk)
	}
	return fks
}
