package orm

import (
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/pg.v4/types"

	"github.com/jinzhu/inflection"
)

type Table struct {
	Name      types.Q // escaped table name
	ModelName string
	Type      reflect.Type

	PKs       []*Field
	Fields    []*Field
	FieldsMap map[string]*Field

	Methods   map[string]*Method
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
		Name:      types.AppendField(nil, inflection.Plural(tableName), 1),
		ModelName: tableName,
		Type:      typ,
		Fields:    make([]*Field, 0, typ.NumField()),
		FieldsMap: make(map[string]*Field, typ.NumField()),
	}
	Tables.inFlight[typ] = table

	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)

		if f.Anonymous {
			embeddedTable := newTable(indirectType(f.Type))

			for _, field := range embeddedTable.Fields {
				field = field.Copy()
				field.Index = append(f.Index, field.Index...)
				if field.Has(PrimaryKeyFlag) {
					table.PKs = append(table.PKs, field)
				}
				table.AddField(field)
			}

			continue
		}

		if f.PkgPath != "" && !f.Anonymous {
			continue
		}

		field := table.newField(f)
		if field != nil {
			table.AddField(field)
		}
	}

	typ = reflect.PtrTo(typ)
	if table.Methods == nil {
		table.Methods = make(map[string]*Method)
	}
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

		retType := m.Type.Out(0)
		method := Method{
			Index: m.Index,

			appender: types.Appender(retType),
		}

		if retType == queryType || retType == fieldType {
			method.flags |= FormatFlag
		}

		table.Methods[m.Name] = &method
	}

	Tables.tables[typ] = table
	delete(Tables.inFlight, typ)

	return table
}

func (t *Table) getField(name string) *Field {
	for _, f := range t.Fields {
		if f.GoName == name {
			return f
		}
	}

	f, ok := t.Type.FieldByName(name)
	if !ok {
		return nil
	}
	return t.newField(f)
}

func (t *Table) newField(f reflect.StructField) *Field {
	sqlName, sqlOpt := parseTag(f.Tag.Get("sql"))

	if f.Name == "TableName" {
		t.Name = types.AppendField(nil, sqlName, 1)
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

	var appender types.AppenderFunc
	var scanner types.ScannerFunc
	fieldType := indirectType(f.Type)
	if _, ok := pgOpt.Get("array"); ok {
		appender = types.ArrayAppender(fieldType)
		scanner = types.ArrayScanner(fieldType)
	} else {
		appender = types.Appender(fieldType)
		scanner = types.Scanner(fieldType)
	}

	field := Field{
		GoName:  f.Name,
		SQLName: sqlName,
		ColName: types.AppendField(nil, sqlName, 1),

		Index: f.Index,

		append: appender,
		scan:   scanner,

		isEmpty: isEmptier(fieldType),
	}

	if skip {
		t.FieldsMap[field.SQLName] = &field
		return nil
	}

	if _, ok := sqlOpt.Get("null"); ok {
		field.flags |= NullFlag
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

	if fieldType == queryType || fieldType == fieldType {
		field.flags |= FormatFlag
	}

	switch fieldType.Kind() {
	case reflect.Slice:
		if fieldType.Elem().Kind() == reflect.Struct {
			joinTable := newTable(fieldType.Elem())

			basePrefix := t.Type.Name()
			if s, _ := pgOpt.Get("fk:"); s != "" {
				basePrefix = s
			}

			if m2mTable, _ := pgOpt.Get("many2many:"); m2mTable != "" {
				joinPrefix := joinTable.Type.Name()
				if s, _ := pgOpt.Get("joinFK:"); s != "" {
					joinPrefix = s
				}

				t.addRelation(&Relation{
					Field:        &field,
					Join:         joinTable,
					M2MTableName: types.AppendField(nil, m2mTable, 1),
					BasePrefix:   Underscore(basePrefix + "_"),
					JoinPrefix:   Underscore(joinPrefix + "_"),
				})
				return nil
			}

			var polymorphic bool
			if s, _ := pgOpt.Get("polymorphic:"); s != "" {
				basePrefix = s
				polymorphic = true
			}

			var fks []*Field
			for _, pk := range t.PKs {
				fkName := basePrefix + pk.GoName
				if fk := joinTable.getField(fkName); fk != nil {
					fks = append(fks, fk)
				}
			}

			if len(fks) > 0 {
				t.addRelation(&Relation{
					Polymorphic: polymorphic,
					Field:       &field,
					FKs:         fks,
					Join:        joinTable,
					BasePrefix:  Underscore(basePrefix + "_"),
				})
				return nil
			}
		}
	case reflect.Struct:
		joinTable := newTable(fieldType)
		if len(joinTable.Fields) == 0 {
			break
		}

		for _, ff := range joinTable.FieldsMap {
			ff = ff.Copy()
			ff.SQLName = field.SQLName + "__" + ff.SQLName
			ff.ColName = types.AppendField(nil, ff.SQLName, 1)
			ff.Index = append(field.Index, ff.Index...)
			t.FieldsMap[ff.SQLName] = ff
		}

		var fks []*Field
		for _, pk := range joinTable.PKs {
			fkName := field.GoName + pk.GoName
			if fk := t.getField(fkName); fk != nil {
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
