package orm

import (
	"reflect"
	"strings"

	"gopkg.in/pg.v3/pgutil"
	"gopkg.in/pg.v3/types"
)

type Table struct {
	Name string

	PK        *Field
	Fields    []*Field
	FieldsMap map[string]*Field

	Methods map[string]*method

	HasOne  map[string]*Field
	HasMany map[string]*Field
}

func (t *Table) AddField(field *Field) {
	t.Fields = append(t.Fields, field)
	t.FieldsMap[field.SQLName] = field
}

func (t *Table) DeleteField(field *Field) {
	for i, f := range t.Fields {
		if f == field {
			t.Fields = append(t.Fields[:i], t.Fields[i+1:]...)
		}
	}
	delete(t.FieldsMap, field.SQLName)
}

func (t *Table) hasOne(field *Field) {
	if t.HasOne == nil {
		t.HasOne = make(map[string]*Field)
	}
	t.HasOne[field.SQLName] = field
}

func (t *Table) hasMany(field *Field) {
	if t.HasMany == nil {
		t.HasMany = make(map[string]*Field)
	}
	t.HasMany[field.SQLName] = field
}

func NewTable(typ reflect.Type) *Table {
	table := &Table{
		Name:      pgutil.Underscore(typ.Name()),
		Fields:    make([]*Field, 0, typ.NumField()),
		FieldsMap: make(map[string]*Field, typ.NumField()),
	}

loop:
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)

		if f.Anonymous {
			typ := f.Type
			if typ.Kind() == reflect.Ptr {
				typ = typ.Elem()
			}
			for _, ff := range NewTable(typ).Fields {
				ff.Index = append(f.Index, ff.Index...)
				table.AddField(ff)
			}
			continue
		}

		if f.PkgPath != "" {
			continue
		}

		_, pgOpt := parseTag(f.Tag.Get("pg"))
		sqlName, sqlOpt := parseTag(f.Tag.Get("sql"))
		if sqlName == "-" {
			continue
		}

		ftype := indirectType(f.Type)
		field := Field{
			GoName:  f.Name,
			SQLName: sqlName,

			Index: f.Index,

			appender: types.Appender(ftype),
			decoder:  types.Decoder(ftype),
		}

		if field.SQLName == "" {
			field.SQLName = pgutil.Underscore(field.GoName)
		}

		if pgOpt.Contains("nullempty") {
			field.flags |= NullEmptyFlag
		}
		if sqlOpt.Contains("pk") || field.SQLName == "id" {
			field.flags |= PrimaryKeyFlag
			table.PK = &field
		} else if strings.HasSuffix(field.SQLName, "_id") {
			field.flags |= ForeignKeyFlag
		}

		switch ftype.Kind() {
		case reflect.Slice:
			if ftype.Elem().Kind() == reflect.Struct {
				fk := typ.Name() + "Id"
				if _, ok := ftype.Elem().FieldByName(fk); ok {
					table.hasMany(&field)
					continue loop
				}
			}
		case reflect.Struct:
			for _, ff := range Tables.Get(ftype).Fields {
				ff = ff.Copy()
				ff.SQLName = field.SQLName + "__" + ff.SQLName
				ff.Index = append(field.Index, ff.Index...)
				table.FieldsMap[ff.SQLName] = ff
			}

			fk := f.Name + "Id"
			if _, ok := typ.FieldByName(fk); ok {
				table.hasOne(&field)
			}

			table.FieldsMap[field.SQLName] = &field
			continue loop
		}

		table.AddField(&field)
	}

	typ = reflect.PtrTo(typ)
	table.Methods = make(map[string]*method, typ.NumMethod())
	for i := 0; i < typ.NumMethod(); i++ {
		m := typ.Method(i)
		if m.Type.NumIn() > 1 {
			continue
		}
		if m.Type.NumOut() != 1 {
			continue
		}
		method := &method{
			Index: m.Index,

			appender: types.Appender(m.Type.Out(0)),
		}
		table.Methods[m.Name] = method
	}

	return table
}
