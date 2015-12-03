package orm

import (
	"reflect"
	"strings"

	"gopkg.in/pg.v3/pgutil"
	"gopkg.in/pg.v3/types"
)

type Table struct {
	Name string

	List    []*Field
	Map     map[string]*Field
	Methods map[string]*method
	HasOne  map[string]struct{}
	HasMany map[string]struct{}
}

func (fs *Table) Len() int {
	return len(fs.List)
}

func (fs *Table) AddField(field *Field) {
	fs.List = append(fs.List, field)
	fs.Map[field.Name] = field
}

func (t *Table) DeleteField(field *Field) {
	for i, f := range t.List {
		if f == field {
			t.List = append(t.List[:i], t.List[i+1:]...)
		}
	}
	delete(t.Map, field.Name)
}

func (t *Table) hasOne(name string) {
	if t.HasOne == nil {
		t.HasOne = make(map[string]struct{})
	}
	t.HasOne[name] = struct{}{}
}

func (t *Table) hasMany(name string) {
	if t.HasMany == nil {
		t.HasMany = make(map[string]struct{})
	}
	t.HasMany[name] = struct{}{}
}

func NewTable(typ reflect.Type) *Table {
	table := &Table{
		Name: pgutil.Underscore(typ.Name()),
		List: make([]*Field, 0, typ.NumField()),
		Map:  make(map[string]*Field, typ.NumField()),
	}

loop:
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)

		if f.Anonymous {
			typ := f.Type
			if typ.Kind() == reflect.Ptr {
				typ = typ.Elem()
			}
			for _, ff := range NewTable(typ).List {
				ff.index = append(f.Index, ff.index...)
				table.AddField(ff)
			}
			continue
		}

		if f.PkgPath != "" {
			continue
		}

		name, pgOpt := parseTag(f.Tag.Get("pg"))
		if name == "-" {
			continue
		}

		sqlName, sqlOpt := parseTag(f.Tag.Get("sql"))
		if sqlName == "-" {
			continue
		}

		fieldType := indirectType(f.Type)
		field := Field{
			Name:    name,
			SQLName: sqlName,

			index: f.Index,

			appender: types.Appender(fieldType),
			decoder:  types.Decoder(fieldType),
		}

		if field.Name == "" {
			field.Name = pgutil.Underscore(f.Name)
		}
		if field.SQLName == "" {
			field.SQLName = field.Name
		}

		if pgOpt.Contains("nullempty") {
			field.flags |= nullEmptyFlag
		}
		if sqlOpt.Contains("pk") || field.SQLName == "id" {
			field.flags |= primaryKeyFlag
		} else if strings.HasSuffix(field.SQLName, "_id") {
			field.flags |= foreignKeyFlag

			hasOneName := field.SQLName[:len(field.SQLName)-3]
			if hasOneField, ok := table.Map[hasOneName]; ok {
				fname := pgutil.CamelCase(hasOneField.Name)
				table.hasOne(fname)
				table.DeleteField(hasOneField)
			}
		}

		switch fieldType.Kind() {
		case reflect.Slice:
			fk := table.Name + "_id"
			foreignTable := registry.Table(fieldType.Elem())
			if _, ok := foreignTable.Map[fk]; ok {
				table.hasMany(f.Name)
				continue loop
			}
		case reflect.Struct:
			fk := field.Name + "_id"
			if _, ok := table.Map[fk]; ok {
				table.hasOne(f.Name)
				continue loop
			}
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
