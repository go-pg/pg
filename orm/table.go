package orm

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jinzhu/inflection"

	"gopkg.in/pg.v4/types"
)

type Table struct {
	Name      types.Q
	Alias     types.Q
	ModelName string
	Type      reflect.Type
	flags     int8

	PKs       []*Field
	Fields    []*Field
	FieldsMap map[string]*Field

	Methods map[string]*Method

	Relations map[string]*Relation
}

func (t *Table) Has(flag int8) bool {
	if t == nil {
		return false
	}
	return t.flags&flag != 0
}

func (t *Table) checkPKs() error {
	if len(t.PKs) == 0 {
		return fmt.Errorf("model %s does not have primary keys", t.Type.Name())
	}
	return nil
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

	modelName := Underscore(typ.Name())
	table = &Table{
		Name:      types.AppendField(nil, inflection.Plural(modelName), 1),
		Alias:     types.AppendField(nil, modelName, 1),
		ModelName: modelName,
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

	if typ.Implements(afterQueryHookType) {
		table.flags |= AfterQueryHookFlag
	}
	if typ.Implements(afterSelectHookType) {
		table.flags |= AfterSelectHookFlag
	}
	if typ.Implements(beforeCreateHookType) {
		table.flags |= BeforeCreateHookFlag
	}
	if typ.Implements(afterCreateHookType) {
		table.flags |= AfterCreateHookFlag
	}

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
		if sqlName != "" {
			t.Name = types.AppendField(nil, sqlName, 1)
		}
		if v, ok := sqlOpt.Get("alias:"); ok {
			t.Alias = types.AppendField(nil, v, 1)
		}
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
	if _, ok := pgOpt.Get("array"); ok {
		appender = types.ArrayAppender(f.Type)
		scanner = types.ArrayScanner(f.Type)
	} else if _, ok := pgOpt.Get("hstore"); ok {
		appender = types.HstoreAppender(f.Type)
		scanner = types.HstoreScanner(f.Type)
	} else {
		appender = types.Appender(f.Type)
		scanner = types.Scanner(f.Type)
	}

	field := Field{
		GoName:  f.Name,
		SQLName: sqlName,
		ColName: types.AppendField(nil, sqlName, 1),

		Index: f.Index,

		append: appender,
		scan:   scanner,

		isEmpty: isEmptier(f.Type),
	}

	if _, ok := sqlOpt.Get("null"); ok {
		field.flags |= NullFlag
	}
	if len(t.PKs) == 0 && (field.SQLName == "id" || field.SQLName == "uuid") {
		field.flags |= PrimaryKeyFlag
		t.PKs = append(t.PKs, &field)
	} else if _, ok := sqlOpt.Get("pk"); ok {
		field.flags |= PrimaryKeyFlag
		t.PKs = append(t.PKs, &field)
	} else if strings.HasSuffix(field.SQLName, "_id") {
		field.flags |= ForeignKeyFlag
	}

	if !skip && types.IsSQLScanner(f.Type) {
		return &field
	}

	fieldType := indirectType(f.Type)

	switch fieldType.Kind() {
	case reflect.Slice:
		elemType := indirectType(fieldType.Elem())
		if elemType.Kind() != reflect.Struct {
			break
		}

		joinTable := newTable(elemType)

		basePrefix := t.Type.Name()
		if s, ok := pgOpt.Get("fk:"); ok {
			basePrefix = s
		}

		if m2mTable, _ := pgOpt.Get("many2many:"); m2mTable != "" {
			joinPrefix := joinTable.Type.Name()
			if s, ok := pgOpt.Get("joinFK:"); ok {
				joinPrefix = s
			}

			t.addRelation(&Relation{
				Type:         Many2ManyRelation,
				Field:        &field,
				JoinTable:    joinTable,
				M2MTableName: types.AppendField(nil, m2mTable, 1),
				BasePrefix:   Underscore(basePrefix + "_"),
				JoinPrefix:   Underscore(joinPrefix + "_"),
			})
			return nil
		}

		var polymorphic bool
		if s, ok := pgOpt.Get("polymorphic:"); ok {
			polymorphic = true
			basePrefix = s
		}

		fks := foreignKeys(t, joinTable, basePrefix)
		if len(fks) > 0 {
			t.addRelation(&Relation{
				Type:        HasManyRelation,
				Polymorphic: polymorphic,
				Field:       &field,
				FKs:         fks,
				JoinTable:   joinTable,
				BasePrefix:  Underscore(basePrefix + "_"),
			})
			return nil
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

		if t.detectHasOne(&field, joinTable) ||
			t.detectBelongsToOne(&field, joinTable) {
			t.FieldsMap[field.SQLName] = &field
			return nil
		}
	}

	if skip {
		t.FieldsMap[field.SQLName] = &field
		return nil
	}
	return &field
}

func foreignKeys(base, join *Table, prefix string) []*Field {
	var fks []*Field
	for _, pk := range base.PKs {
		fkName := prefix + pk.GoName
		if fk := join.getField(fkName); fk != nil {
			fks = append(fks, fk)
		}
	}
	return fks
}

func (t *Table) detectHasOne(field *Field, joinTable *Table) bool {
	fks := foreignKeys(joinTable, t, field.GoName)
	if len(fks) > 0 {
		t.addRelation(&Relation{
			Type:      HasOneRelation,
			Field:     field,
			FKs:       fks,
			JoinTable: joinTable,
		})
		return true
	}
	return false
}

func (t *Table) detectBelongsToOne(field *Field, joinTable *Table) bool {
	fks := foreignKeys(t, joinTable, t.Type.Name())
	if len(fks) > 0 {
		t.addRelation(&Relation{
			Type:      BelongsToRelation,
			Field:     field,
			FKs:       fks,
			JoinTable: joinTable,
		})
		return true
	}
	return false
}
