package orm

import (
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/pg.v4/types"

	"github.com/jinzhu/inflection"
)

type Table struct {
	Name      string
	ModelName string
	Type      reflect.Type

	PKs       []*Field
	Fields    []*Field
	FieldsMap map[string]*Field

	Methods   map[string]*method
	Relations map[string]*Relation
}

func (t *Table) AddField(field *Field) {
	t.Fields = append(t.Fields, field)
	t.FieldsMap[field.SQLName] = field
}

func (t *Table) GetField(fieldName string) (*Field, error) {
	field, ok := t.FieldsMap[fieldName]
	if !ok {
		return nil, fmt.Errorf("can't find column %s in table %s", fieldName, t.Name)
	}
	return field, nil
}

func (t *Table) addRelation(rel *Relation) {
	if t.Relations == nil {
		t.Relations = make(map[string]*Relation)
	}
	if rel.M2M != nil {
		rel.M2MBaseFKs = m2mFKs(t, rel.M2M)
		rel.M2MJoinFKs = m2mFKs(rel.Join, rel.M2M)

		rel.addM2MModelFields(rel.M2M.Fields, t.ModelName)
		rel.addM2MModelFields(rel.M2M.Fields, rel.Join.ModelName)
	}
	t.Relations[rel.Field.GoName] = rel
}

func newTable(typ reflect.Type) *Table {
	table, ok := Tables.tables[typ]
	if ok {
		return table
	}

	table, ok = Tables.inFlight[typ]
	if ok {
		return table
	}

	tableName := Underscore(typ.Name())
	table = &Table{
		Name:      inflection.Plural(tableName),
		ModelName: tableName,
		Type:      typ,
		Fields:    make([]*Field, 0, typ.NumField()),
		FieldsMap: make(map[string]*Field, typ.NumField()),
	}
	Tables.inFlight[typ] = table

	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)

		if f.Anonymous {
			typ := f.Type
			if typ.Kind() == reflect.Ptr {
				typ = typ.Elem()
			}
			for _, ff := range newTable(typ).Fields {
				ff = ff.Copy()
				ff.Index = append(f.Index, ff.Index...)
				table.AddField(ff)
			}
			continue
		}

		if f.PkgPath != "" && !f.Anonymous {
			continue
		}

		field := table.newField(typ, f)
		if field != nil {
			table.AddField(field)
		}
	}

	typ = reflect.PtrTo(typ)
	table.Methods = make(map[string]*method)
	for i := 0; i < typ.NumMethod(); i++ {
		m := typ.Method(i)
		if m.PkgPath != "" {
			continue
		}
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

	Tables.tables[typ] = table
	delete(Tables.inFlight, typ)

	return table
}

func (t *Table) newField(typ reflect.Type, f reflect.StructField) *Field {
	sqlName, sqlOpt := parseTag(f.Tag.Get("sql"))

	if f.Name == "TableName" {
		t.Name = sqlName
		return nil
	}

	skip := sqlName == "-"
	if skip || sqlName == "" {
		sqlName = Underscore(f.Name)
	}

	if field, ok := t.FieldsMap[sqlName]; ok {
		return field
	}

	_, pgOpt := parseTag(f.Tag.Get("pg"))

	ftype := indirectType(f.Type)
	field := Field{
		GoName:  f.Name,
		SQLName: sqlName,

		Index: f.Index,

		appender: types.Appender(ftype),
		decoder:  types.Decoder(ftype),
	}

	if skip {
		t.FieldsMap[field.SQLName] = &field
		return nil
	}

	if _, ok := pgOpt.Get("nullempty"); ok {
		field.flags |= NullEmptyFlag
	}
	if field.SQLName == "id" {
		field.flags |= PrimaryKeyFlag
		t.PKs = append(t.PKs, &field)
	} else if _, ok := sqlOpt.Get("pk"); ok {
		field.flags |= PrimaryKeyFlag
		t.PKs = append(t.PKs, &field)
	} else if strings.HasSuffix(field.SQLName, "_id") {
		field.flags |= ForeignKeyFlag
	}

	var polymorphic string
	if s, _ := pgOpt.Get("polymorphic:"); s != "" {
		polymorphic = Underscore(s) + "_"
	}

	switch ftype.Kind() {
	case reflect.Slice:
		if ftype.Elem().Kind() == reflect.Struct {
			joinTable := newTable(ftype.Elem())

			if m2mName, _ := pgOpt.Get("many2many:"); m2mName != "" {
				if m2mSlice, ok := typ.FieldByName(m2mName); ok {
					t.addRelation(&Relation{
						Field: &field,
						Join:  joinTable,
						M2M:   newTable(m2mSlice.Type.Elem()),
					})
				}
				return nil
			}

			var fks []*Field
			var prefix string
			if polymorphic != "" {
				prefix = polymorphic
			} else {
				prefix = t.ModelName + "_"
			}

			for _, pk := range t.PKs {
				fkName := prefix + pk.SQLName
				fk, ok := joinTable.FieldsMap[fkName]
				if ok {
					fks = append(fks, fk)
				}
			}

			if len(fks) > 0 {
				t.addRelation(&Relation{
					Polymorphic: polymorphic,
					Field:       &field,
					FKs:         fks,
					Join:        joinTable,
				})
				return nil
			}
		}
	case reflect.Struct:
		joinTable := newTable(ftype)
		if len(joinTable.Fields) == 0 {
			break
		}

		for _, ff := range joinTable.Fields {
			ff = ff.Copy()
			ff.SQLName = field.SQLName + "__" + ff.SQLName
			ff.Index = append(field.Index, ff.Index...)
			t.FieldsMap[ff.SQLName] = ff
		}

		var fks []*Field
		for _, pk := range joinTable.PKs {
			fkName := field.SQLName + "_" + pk.SQLName
			fk, ok := t.FieldsMap[fkName]
			if ok {
				fks = append(fks, fk)
			}
		}

		if len(fks) > 0 {
			t.addRelation(&Relation{
				One:   true,
				Field: &field,
				FKs:   fks,
				Join:  joinTable,
			})
		}

		t.FieldsMap[field.SQLName] = &field
		return nil
	}

	return &field
}
