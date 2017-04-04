package orm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/types"

	"github.com/jinzhu/inflection"
)

var nullBool = reflect.TypeOf((*sql.NullBool)(nil)).Elem()
var nullFloat = reflect.TypeOf((*sql.NullFloat64)(nil)).Elem()
var nullInt = reflect.TypeOf((*sql.NullInt64)(nil)).Elem()
var nullString = reflect.TypeOf((*sql.NullString)(nil)).Elem()

type Table struct {
	Type       reflect.Type
	zeroStruct reflect.Value

	TypeName  string
	Name      types.Q
	Alias     types.Q
	ModelName string

	PKs       []*Field
	Fields    []*Field
	FieldsMap map[string]*Field

	Methods   map[string]*Method
	Relations map[string]*Relation

	flags int16
}

func (t *Table) Has(flag int16) bool {
	if t == nil {
		return false
	}
	return t.flags&flag != 0
}

func (t *Table) HasField(field string) bool {
	_, err := t.GetField(field)
	return err == nil
}

func (t *Table) checkPKs() error {
	if len(t.PKs) == 0 {
		return fmt.Errorf("model=%s does not have primary keys", t.Type.Name())
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
		return nil, fmt.Errorf("can't find column=%s in table=%s", fieldName, t.Name)
	}
	return field, nil
}

func (t *Table) AppendParam(dst []byte, strct reflect.Value, name string) ([]byte, bool) {
	if field, ok := t.FieldsMap[name]; ok {
		dst = field.AppendValue(dst, strct, 1)
		return dst, true
	}

	if method, ok := t.Methods[name]; ok {
		dst = method.AppendValue(dst, strct.Addr(), 1)
		return dst, true
	}

	return dst, false
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

	modelName := internal.Underscore(typ.Name())
	table = &Table{
		Type:       typ,
		zeroStruct: reflect.Zero(typ),

		TypeName:  internal.ToExported(typ.Name()),
		Name:      types.Q(types.AppendField(nil, inflection.Plural(modelName), 1)),
		Alias:     types.Q(types.AppendField(nil, modelName, 1)),
		ModelName: modelName,

		Fields:    make([]*Field, 0, typ.NumField()),
		FieldsMap: make(map[string]*Field, typ.NumField()),
	}
	Tables.inFlight[typ] = table

	table.addFields(typ, nil)
	typ = reflect.PtrTo(typ)

	if typ.Implements(afterQueryHookType) {
		table.flags |= AfterQueryHookFlag
	}
	if typ.Implements(afterSelectHookType) {
		table.flags |= AfterSelectHookFlag
	}
	if typ.Implements(beforeInsertHookType) {
		table.flags |= BeforeInsertHookFlag
	}
	if typ.Implements(afterInsertHookType) {
		table.flags |= AfterInsertHookFlag
	}
	if typ.Implements(beforeUpdateHookType) {
		table.flags |= BeforeUpdateHookFlag
	}
	if typ.Implements(afterUpdateHookType) {
		table.flags |= AfterUpdateHookFlag
	}
	if typ.Implements(beforeDeleteHookType) {
		table.flags |= BeforeDeleteHookFlag
	}
	if typ.Implements(afterDeleteHookType) {
		table.flags |= AfterDeleteHookFlag
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

func (t *Table) addFields(typ reflect.Type, index []int) {
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)

		if f.Anonymous {
			embeddedTable := newTable(indirectType(f.Type))

			_, pgOpt := parseTag(f.Tag.Get("pg"))
			if _, ok := pgOpt.Get("override"); ok {
				t.TypeName = embeddedTable.TypeName
				t.Name = embeddedTable.Name
				t.Alias = embeddedTable.Alias
				t.ModelName = embeddedTable.ModelName
			}

			t.addFields(embeddedTable.Type, append(index, f.Index...))
			continue
		}

		field := t.newField(f, index)
		if field != nil {
			t.AddField(field)
		}
	}
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
	return t.newField(f, nil)
}

func (t *Table) newField(f reflect.StructField, index []int) *Field {
	sqlName, sqlOpt := parseTag(f.Tag.Get("sql"))

	switch f.Name {
	case "tableName", "TableName":
		if index != nil {
			return nil
		}
		if sqlName != "" {
			if isPostgresKeyword(sqlName) {
				sqlName = `"` + sqlName + `"`
			}
			t.Name = types.Q(sqlName)
		}
		if alias, ok := sqlOpt.Get("alias:"); ok {
			t.Alias = types.Q(alias)
		}
		return nil
	}

	if f.PkgPath != "" {
		return nil
	}

	skip := sqlName == "-"
	if skip || sqlName == "" {
		sqlName = internal.Underscore(f.Name)
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
		Type: indirectType(f.Type),

		GoName:  f.Name,
		SQLName: sqlName,
		ColName: types.Q(types.AppendField(nil, sqlName, 1)),

		Index: append(index, f.Index...),

		append: appender,
		scan:   scanner,

		isEmpty: isEmptyFunc(f.Type),
	}

	if _, ok := sqlOpt.Get("notnull"); ok {
		field.flags |= NotNullFlag
	}
	if _, ok := sqlOpt.Get("unique"); ok {
		field.flags |= UniqueFlag
	}

	if len(t.PKs) == 0 && (field.SQLName == "id" || field.SQLName == "uuid") {
		field.flags |= PrimaryKeyFlag
		t.PKs = append(t.PKs, &field)
	} else if _, ok := sqlOpt.Get("pk"); ok {
		field.flags |= PrimaryKeyFlag
		t.PKs = append(t.PKs, &field)
	} else if strings.HasSuffix(string(field.SQLName), "_id") {
		field.flags |= ForeignKeyFlag
	}

	field.SQLType = sqlType(&field, sqlOpt)

	if !skip && types.IsSQLScanner(f.Type) {
		return &field
	}

	switch field.Type.Kind() {
	case reflect.Slice:
		elemType := indirectType(field.Type.Elem())
		if elemType.Kind() != reflect.Struct {
			break
		}

		joinTable := newTable(elemType)

		basePrefix := t.TypeName
		if s, ok := pgOpt.Get("fk:"); ok {
			basePrefix = s
		}

		if m2mTable, _ := pgOpt.Get("many2many:"); m2mTable != "" {
			joinPrefix := joinTable.TypeName
			if s, ok := pgOpt.Get("joinFK:"); ok {
				joinPrefix = s
			}

			t.addRelation(&Relation{
				Type:         Many2ManyRelation,
				Field:        &field,
				JoinTable:    joinTable,
				M2MTableName: types.Q(m2mTable),
				BasePrefix:   internal.Underscore(basePrefix + "_"),
				JoinPrefix:   internal.Underscore(joinPrefix + "_"),
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
				BasePrefix:  internal.Underscore(basePrefix + "_"),
			})
			return nil
		}
	case reflect.Struct:
		joinTable := newTable(field.Type)
		if len(joinTable.Fields) == 0 {
			break
		}

		for _, ff := range joinTable.FieldsMap {
			ff = ff.Copy()
			ff.SQLName = field.SQLName + "__" + ff.SQLName
			ff.ColName = types.Q(types.AppendField(nil, ff.SQLName, 1))
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

func isPostgresKeyword(s string) bool {
	switch s {
	case "user":
		return true
	}
	return false
}

func sqlType(field *Field, sqlOpt tagOptions) string {
	if v, ok := sqlOpt.Get("type:"); ok {
		return v
	}

	switch field.Type {
	case timeType:
		return "timestamptz"
	case nullBool:
		return "boolean"
	case nullFloat:
		return "double precision"
	case nullInt:
		return "bigint"
	case nullString:
		return "text"
	}

	switch field.Type.Kind() {
	case reflect.Int8, reflect.Uint8, reflect.Int16:
		if field.Has(PrimaryKeyFlag) {
			return "smallserial"
		}
		return "smallint"
	case reflect.Uint16, reflect.Int32:
		if field.Has(PrimaryKeyFlag) {
			return "serial"
		}
		return "integer"
	case reflect.Uint32, reflect.Int64, reflect.Int:
		if field.Has(PrimaryKeyFlag) {
			return "bigserial"
		}
		return "bigint"
	case reflect.Uint, reflect.Uint64:
		return "decimal"
	case reflect.Float32:
		return "real"
	case reflect.Float64:
		return "double precision"
	case reflect.Bool:
		return "boolean"
	case reflect.String:
		return "text"
	case reflect.Map, reflect.Struct:
		return "jsonb"
	case reflect.Array, reflect.Slice:
		if field.Type.Elem().Kind() == reflect.Uint8 {
			return "bytea"
		}
		return "jsonb"
	default:
		return field.Type.Kind().String()
	}
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
	fks := foreignKeys(t, joinTable, t.TypeName)
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
