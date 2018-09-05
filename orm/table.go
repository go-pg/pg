package orm

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strings"
	"time"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/types"
)

const (
	AfterQueryHookFlag = uint16(1) << iota
	BeforeSelectQueryHookFlag
	AfterSelectHookFlag
	BeforeInsertHookFlag
	AfterInsertHookFlag
	BeforeUpdateHookFlag
	AfterUpdateHookFlag
	BeforeDeleteHookFlag
	AfterDeleteHookFlag
	discardUnknownColumns
	softDelete
)

var timeType = reflect.TypeOf((*time.Time)(nil)).Elem()
var ipType = reflect.TypeOf((*net.IP)(nil)).Elem()
var ipNetType = reflect.TypeOf((*net.IPNet)(nil)).Elem()
var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
var nullBoolType = reflect.TypeOf((*sql.NullBool)(nil)).Elem()
var nullFloatType = reflect.TypeOf((*sql.NullFloat64)(nil)).Elem()
var nullIntType = reflect.TypeOf((*sql.NullInt64)(nil)).Elem()
var nullStringType = reflect.TypeOf((*sql.NullString)(nil)).Elem()

// Table represents a SQL table created from Go struct.
type Table struct {
	Type       reflect.Type
	zeroStruct reflect.Value

	TypeName       string
	Name           types.Q
	NameForSelects types.Q
	Alias          types.Q
	ModelName      string

	allFields     []*Field // read only
	skippedFields []*Field

	Fields     []*Field // PKs + DataFields
	PKs        []*Field
	DataFields []*Field
	FieldsMap  map[string]*Field

	Methods   map[string]*Method
	Relations map[string]*Relation
	Unique    map[string][]*Field

	flags uint16
}

func (t *Table) setName(name types.Q) {
	t.Name = name
	t.NameForSelects = name
}

func newTable(typ reflect.Type) *Table {
	t := new(Table)
	t.Type = typ
	t.zeroStruct = reflect.New(t.Type).Elem()
	t.TypeName = internal.ToExported(t.Type.Name())
	t.ModelName = internal.Underscore(t.Type.Name())
	tableName := quoteTableName(tableNameInflector(t.ModelName))
	t.setName(types.Q(types.AppendField(nil, tableName, 1)))
	t.Alias = types.Q(types.AppendField(nil, t.ModelName, 1))

	typ = reflect.PtrTo(t.Type)
	if typ.Implements(afterQueryHookType) {
		t.SetFlag(AfterQueryHookFlag)
	}
	if typ.Implements(beforeSelectQueryHookType) {
		t.SetFlag(BeforeSelectQueryHookFlag)
	}
	if typ.Implements(afterSelectHookType) {
		t.SetFlag(AfterSelectHookFlag)
	}
	if typ.Implements(beforeInsertHookType) {
		t.SetFlag(BeforeInsertHookFlag)
	}
	if typ.Implements(afterInsertHookType) {
		t.SetFlag(AfterInsertHookFlag)
	}
	if typ.Implements(beforeUpdateHookType) {
		t.SetFlag(BeforeUpdateHookFlag)
	}
	if typ.Implements(afterUpdateHookType) {
		t.SetFlag(AfterUpdateHookFlag)
	}
	if typ.Implements(beforeDeleteHookType) {
		t.SetFlag(BeforeDeleteHookFlag)
	}
	if typ.Implements(afterDeleteHookType) {
		t.SetFlag(AfterDeleteHookFlag)
	}

	t.initFields()
	t.initMethods()

	return t
}

func (t *Table) String() string {
	return "model=" + t.TypeName
}

func (t *Table) SetFlag(flag uint16) {
	t.flags |= flag
}

func (t *Table) HasFlag(flag uint16) bool {
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
		return fmt.Errorf("pg: %s does not have primary keys", t)
	}
	return nil
}

func (t *Table) mustSoftDelete() error {
	if !t.HasFlag(softDelete) {
		return fmt.Errorf("pg: %s does not support soft deletes", t)
	}
	return nil
}

func (t *Table) AddField(field *Field) {
	t.allFields = append(t.allFields, field)
	t.Fields = append(t.Fields, field)
	if field.HasFlag(PrimaryKeyFlag) {
		t.PKs = append(t.PKs, field)
	} else {
		t.DataFields = append(t.DataFields, field)
	}
	t.FieldsMap[field.SQLName] = field
}

func (t *Table) RemoveField(field *Field) {
	t.Fields = removeField(t.Fields, field)
	if field.HasFlag(PrimaryKeyFlag) {
		t.PKs = removeField(t.PKs, field)
	} else {
		t.DataFields = removeField(t.DataFields, field)
	}
	delete(t.FieldsMap, field.SQLName)
}

func removeField(fields []*Field, field *Field) []*Field {
	for i, f := range fields {
		if f == field {
			fields = append(fields[:i], fields[i+1:]...)
		}
	}
	return fields
}

func (t *Table) GetField(fieldName string) (*Field, error) {
	field, ok := t.FieldsMap[fieldName]
	if !ok {
		return nil, fmt.Errorf("can't find column=%s in %s", fieldName, t)
	}
	return field, nil
}

func (t *Table) AppendParam(b []byte, strct reflect.Value, name string) ([]byte, bool) {
	field, ok := t.FieldsMap[name]
	if ok {
		b = field.AppendValue(b, strct, 1)
		return b, true
	}

	method, ok := t.Methods[name]
	if ok {
		b = method.AppendValue(b, strct.Addr(), 1)
		return b, true
	}

	return b, false
}

func (t *Table) initFields() {
	t.Fields = make([]*Field, 0, t.Type.NumField())
	t.FieldsMap = make(map[string]*Field, t.Type.NumField())
	t.addFields(t.Type, nil)
}

func (t *Table) addFields(typ reflect.Type, baseIndex []int) {
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)

		// Make a copy so slice is not shared between fields.
		index := make([]int, len(baseIndex))
		copy(index, baseIndex)

		if f.Anonymous {
			sqlTag := f.Tag.Get("sql")
			if sqlTag == "-" {
				continue
			}

			fieldType := indirectType(f.Type)
			t.addFields(fieldType, append(index, f.Index...))

			pgTag := parseTag(f.Tag.Get("pg"))
			_, inherit := pgTag.Options["inherit"]
			_, override := pgTag.Options["override"]
			if inherit || override {
				embeddedTable := newTable(fieldType)
				t.TypeName = embeddedTable.TypeName
				t.Name = embeddedTable.Name
				t.NameForSelects = embeddedTable.NameForSelects
				t.Alias = embeddedTable.Alias
				t.ModelName = embeddedTable.ModelName
			}

			continue
		}

		field := t.newField(f, index)
		if field != nil {
			t.AddField(field)
		}
	}
}

func (t *Table) newField(f reflect.StructField, index []int) *Field {
	sqlTag := parseTag(f.Tag.Get("sql"))

	switch f.Name {
	case "tableName", "TableName":
		if len(index) > 0 {
			return nil
		}

		if sqlTag.Name != "" {
			s, _ := unquoteTagValue(sqlTag.Name)
			t.setName(types.Q(quoteTableName(s)))
		}

		if v, ok := sqlTag.Options["select"]; ok {
			v, _ = unquoteTagValue(v)
			t.NameForSelects = types.Q(quoteTableName(v))
		}

		if v, ok := sqlTag.Options["alias"]; ok {
			v, _ = unquoteTagValue(v)
			t.Alias = types.Q(quoteTableName(v))
		}

		pgTag := parseTag(f.Tag.Get("pg"))
		if _, ok := pgTag.Options["discard_unknown_columns"]; ok {
			t.SetFlag(discardUnknownColumns)
		}

		return nil
	}

	if f.PkgPath != "" {
		return nil
	}

	skip := sqlTag.Name == "-"
	if skip || sqlTag.Name == "" {
		sqlTag.Name = internal.Underscore(f.Name)
	}

	index = append(index, f.Index...)
	if field, ok := t.FieldsMap[sqlTag.Name]; ok {
		if indexEqual(field.Index, index) {
			return field
		}
		t.RemoveField(field)
	}

	field := &Field{
		Field: f,
		Type:  indirectType(f.Type),

		GoName:  f.Name,
		SQLName: sqlTag.Name,
		Column:  types.Q(types.AppendField(nil, sqlTag.Name, 1)),

		Index: index,
	}

	if _, ok := sqlTag.Options["notnull"]; ok {
		field.SetFlag(NotNullFlag)
	}
	if v, ok := sqlTag.Options["unique"]; ok {
		if v == "" {
			field.SetFlag(UniqueFlag)
		} else {
			if t.Unique == nil {
				t.Unique = make(map[string][]*Field)
			}
			t.Unique[v] = append(t.Unique[v], field)
		}
	}
	if v, ok := sqlTag.Options["default"]; ok {
		v, ok = unquoteTagValue(v)
		if ok {
			field.Default = types.Q(types.AppendString(nil, v, 1))
		} else {
			field.Default = types.Q(v)
		}
	}

	if _, ok := sqlTag.Options["pk"]; ok {
		field.SetFlag(PrimaryKeyFlag)
	} else if strings.HasSuffix(field.SQLName, "_id") ||
		strings.HasSuffix(field.SQLName, "_uuid") {
		field.SetFlag(ForeignKeyFlag)
	} else if strings.HasPrefix(field.SQLName, "fk_") {
		field.SetFlag(ForeignKeyFlag)
	} else if len(t.PKs) == 0 {
		if field.SQLName == "id" ||
			field.SQLName == "uuid" ||
			field.SQLName == "pk_"+t.ModelName {
			field.SetFlag(PrimaryKeyFlag)
		}
	}

	pgTag := parseTag(f.Tag.Get("pg"))

	if _, ok := sqlTag.Options["array"]; ok {
		field.SetFlag(ArrayFlag)
	} else if _, ok := pgTag.Options["array"]; ok {
		field.SetFlag(ArrayFlag)
	}

	field.SQLType = fieldSQLType(field, pgTag, sqlTag)
	if strings.HasSuffix(field.SQLType, "[]") {
		field.SetFlag(ArrayFlag)
	}

	if v, ok := sqlTag.Options["on_delete"]; ok {
		field.OnDelete = v
	}

	if v, ok := sqlTag.Options["composite"]; ok {
		field.SQLType = v
		field.append = compositeAppender(f.Type)
		field.scan = compositeScanner(f.Type)
	} else if _, ok := pgTag.Options["json_use_number"]; ok {
		field.append = types.Appender(f.Type)
		field.scan = scanJSONValue
	} else if field.HasFlag(ArrayFlag) {
		field.append = types.ArrayAppender(f.Type)
		field.scan = types.ArrayScanner(f.Type)
	} else if _, ok := sqlTag.Options["hstore"]; ok {
		field.append = types.HstoreAppender(f.Type)
		field.scan = types.HstoreScanner(f.Type)
	} else if _, ok := pgTag.Options["hstore"]; ok {
		field.append = types.HstoreAppender(f.Type)
		field.scan = types.HstoreScanner(f.Type)
	} else {
		field.append = types.Appender(f.Type)
		field.scan = types.Scanner(f.Type)
	}
	field.isZero = isZeroFunc(f.Type)

	if skip {
		t.skippedFields = append(t.skippedFields, field)
		t.FieldsMap[field.SQLName] = field
		return nil
	}

	switch field.SQLName {
	case "deleted_at":
		if _, ok := pgTag.Options["soft_delete"]; ok && field.Type == timeType {
			t.SetFlag(softDelete)
		}
	}

	return field
}

func (t *Table) initMethods() {
	t.Methods = make(map[string]*Method)
	typ := reflect.PtrTo(t.Type)
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
		t.Methods[m.Name] = &Method{
			Index: m.Index,

			appender: types.Appender(retType),
		}
	}
}

func (t *Table) init() {
	t.initRelations()
	t.initInlines()
}

func (t *Table) initRelations() {
	for i := 0; i < len(t.Fields); {
		f := t.Fields[i]
		if t.tryRelation(f) {
			t.Fields = removeField(t.Fields, f)
			t.DataFields = removeField(t.DataFields, f)
		} else {
			i++
		}
	}
}

func (t *Table) initInlines() {
	for _, f := range t.skippedFields {
		if f.Type.Kind() == reflect.Struct {
			joinTable := _tables.get(f.Type, true)
			t.inlineFields(f, joinTable.allFields)
		}
	}
}

func (t *Table) tryRelation(field *Field) bool {
	if isColumn(field.Type) {
		return false
	}

	switch field.Type.Kind() {
	case reflect.Slice:
		return t.tryRelationSlice(field)
	case reflect.Struct:
		return t.tryRelationStruct(field)
	}
	return false
}

func (t *Table) tryRelationSlice(field *Field) bool {
	elemType := indirectType(field.Type.Elem())
	if elemType.Kind() != reflect.Struct {
		return false
	}

	pgTag := parseTag(field.Field.Tag.Get("pg"))
	joinTable := _tables.get(elemType, true)

	fk, fkOK := pgTag.Options["fk"]
	if fkOK {
		if fk == "-" {
			return false
		}
		fk = tryUnderscorePrefix(fk)
	}

	if m2mTableName, _ := pgTag.Options["many2many"]; m2mTableName != "" {
		m2mTable := _tables.getByName(m2mTableName)

		var m2mTableAlias types.Q
		if m2mTable != nil {
			m2mTableAlias = m2mTable.Alias
		} else if ind := strings.IndexByte(m2mTableName, '.'); ind >= 0 {
			m2mTableAlias = types.Q(m2mTableName[ind+1:])
		} else {
			m2mTableAlias = types.Q(m2mTableName)
		}

		var fks []string
		if !fkOK {
			fk = t.ModelName + "_"
		}
		if m2mTable != nil {
			keys := foreignKeys(t, m2mTable, fk, fkOK)
			if len(keys) == 0 {
				return false
			}
			for _, fk := range keys {
				fks = append(fks, fk.SQLName)
			}
		} else {
			if fkOK && len(t.PKs) == 1 {
				fks = append(fks, fk)
			} else {
				for _, pk := range t.PKs {
					fks = append(fks, fk+pk.SQLName)
				}
			}
		}

		joinFK, joinFKOK := pgTag.Options["joinFK"]
		if joinFKOK {
			joinFK = tryUnderscorePrefix(joinFK)
		} else {
			joinFK = joinTable.ModelName + "_"
		}
		var joinFKs []string
		if m2mTable != nil {
			keys := foreignKeys(joinTable, m2mTable, joinFK, joinFKOK)
			if len(keys) == 0 {
				return false
			}
			for _, fk := range keys {
				joinFKs = append(joinFKs, fk.SQLName)
			}
		} else {
			if joinFKOK && len(joinTable.PKs) == 1 {
				joinFKs = append(joinFKs, joinFK)
			} else {
				for _, pk := range joinTable.PKs {
					joinFKs = append(joinFKs, joinFK+pk.SQLName)
				}
			}
		}

		t.addRelation(&Relation{
			Type:          Many2ManyRelation,
			Field:         field,
			JoinTable:     joinTable,
			M2MTableName:  types.Q(m2mTableName),
			M2MTableAlias: m2mTableAlias,
			BaseFKs:       fks,
			JoinFKs:       joinFKs,
		})
		return true
	}

	s, polymorphic := pgTag.Options["polymorphic"]
	var typeField *Field
	if polymorphic {
		fk = tryUnderscorePrefix(s)

		typeField = joinTable.getField(fk + "type")
		if typeField == nil {
			return false
		}
	} else if !fkOK {
		fk = t.ModelName + "_"
	}

	fks := foreignKeys(t, joinTable, fk, fkOK || polymorphic)
	if len(fks) == 0 {
		return false
	}

	var fkValues []*Field
	fkValue, ok := pgTag.Options["fk_value"]
	if ok {
		if len(fks) > 1 {
			panic(fmt.Errorf("got fk_value, but there are %d fks", len(fks)))
		}

		f := t.getField(fkValue)
		if f == nil {
			panic(fmt.Errorf("fk_value=%q not found in %s", fkValue, t))
		}
		fkValues = append(fkValues, f)
	} else {
		fkValues = t.PKs
	}

	if len(fks) > 0 {
		t.addRelation(&Relation{
			Type:        HasManyRelation,
			Field:       field,
			JoinTable:   joinTable,
			FKs:         fks,
			Polymorphic: typeField,
			FKValues:    fkValues,
		})
		return true
	}

	return false
}

func (t *Table) tryRelationStruct(field *Field) bool {
	pgTag := parseTag(field.Field.Tag.Get("pg"))
	joinTable := _tables.get(field.Type, true)
	if len(joinTable.allFields) == 0 {
		return false
	}

	res := t.tryHasOne(joinTable, field, pgTag) ||
		t.tryBelongsToOne(joinTable, field, pgTag)
	t.inlineFields(field, joinTable.allFields)
	return res
}

func (t *Table) inlineFields(strct *Field, structFields []*Field) {
	for _, f := range structFields {
		f = f.Copy()
		f.GoName = strct.GoName + "_" + f.GoName
		f.SQLName = strct.SQLName + "__" + f.SQLName
		f.Column = types.Q(types.AppendField(nil, f.SQLName, 1))
		f.Index = appendNew(strct.Index, f.Index...)
		if _, ok := t.FieldsMap[f.SQLName]; !ok {
			t.FieldsMap[f.SQLName] = f
		}
	}
}

func appendNew(dst []int, src ...int) []int {
	cp := make([]int, len(dst)+len(src))
	copy(cp, dst)
	copy(cp[len(dst):], src)
	return cp
}

func isPostgresKeyword(s string) bool {
	switch strings.ToLower(s) {
	case "user", "group", "constraint", "limit",
		"member", "placing", "references", "table":
		return true
	default:
		return false
	}
}

func isColumn(typ reflect.Type) bool {
	return typ.Implements(scannerType) || reflect.PtrTo(typ).Implements(scannerType)
}

func fieldSQLType(field *Field, pgTag, sqlTag *tag) string {
	if typ, ok := sqlTag.Options["type"]; ok {
		field.SetFlag(customTypeFlag)
		typ, _ = unquoteTagValue(typ)
		return typ
	}

	if _, ok := sqlTag.Options["hstore"]; ok {
		field.SetFlag(customTypeFlag)
		return "hstore"
	} else if _, ok := pgTag.Options["hstore"]; ok {
		field.SetFlag(customTypeFlag)
		return "hstore"
	}

	if field.HasFlag(ArrayFlag) {
		sqlType := sqlType(field.Type.Elem())
		return sqlType + "[]"
	}

	sqlType := sqlType(field.Type)
	if field.HasFlag(PrimaryKeyFlag) {
		return pkSQLType(sqlType)
	}

	switch sqlType {
	case "timestamptz":
		field.SetFlag(customTypeFlag)
	}

	return sqlType
}

func sqlType(typ reflect.Type) string {
	switch typ {
	case timeType:
		return "timestamptz"
	case ipType:
		return "inet"
	case ipNetType:
		return "cidr"
	case nullBoolType:
		return "boolean"
	case nullFloatType:
		return "double precision"
	case nullIntType:
		return "bigint"
	case nullStringType:
		return "text"
	}

	switch typ.Kind() {
	case reflect.Int8, reflect.Uint8, reflect.Int16:
		return "smallint"
	case reflect.Uint16, reflect.Int32:
		return "integer"
	case reflect.Uint32, reflect.Int64, reflect.Int:
		return "bigint"
	case reflect.Uint, reflect.Uint64:
		// The lesser of two evils.
		return "bigint"
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
		if typ.Elem().Kind() == reflect.Uint8 {
			return "bytea"
		}
		return "jsonb"
	default:
		return typ.Kind().String()
	}
}

func pkSQLType(s string) string {
	switch s {
	case "smallint":
		return "smallserial"
	case "integer":
		return "serial"
	case "bigint":
		return "bigserial"
	}
	return s
}

func sqlTypeEqual(a, b string) bool {
	if a == b {
		return true
	}
	return pkSQLType(a) == pkSQLType(b)
}

func (t *Table) tryHasOne(joinTable *Table, field *Field, tag *tag) bool {
	fk, fkOK := tag.Options["fk"]
	if fkOK {
		if fk == "-" {
			return false
		}
		fk = tryUnderscorePrefix(fk)
	} else {
		fk = internal.Underscore(field.GoName) + "_"
	}

	fks := foreignKeys(joinTable, t, fk, fkOK)
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

func (t *Table) tryBelongsToOne(joinTable *Table, field *Field, tag *tag) bool {
	fk, fkOK := tag.Options["fk"]
	if fkOK {
		if fk == "-" {
			return false
		}
		fk = tryUnderscorePrefix(fk)
	} else {
		fk = internal.Underscore(t.TypeName) + "_"
	}

	fks := foreignKeys(t, joinTable, fk, fkOK)
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

func (t *Table) addRelation(rel *Relation) {
	if t.Relations == nil {
		t.Relations = make(map[string]*Relation)
	}
	_, ok := t.Relations[rel.Field.GoName]
	if ok {
		panic(fmt.Errorf("%s already has %s", t, rel))
	}
	t.Relations[rel.Field.GoName] = rel
}

func foreignKeys(base, join *Table, fk string, tryFK bool) []*Field {
	var fks []*Field

	for _, pk := range base.PKs {
		fkName := fk + pk.SQLName
		f := join.getField(fkName)
		if f != nil && sqlTypeEqual(pk.SQLType, f.SQLType) {
			fks = append(fks, f)
		}
	}
	if len(fks) > 0 {
		return fks
	}

	for _, pk := range base.PKs {
		if !strings.HasPrefix(pk.SQLName, "pk_") {
			continue
		}
		fkName := "fk_" + pk.SQLName[3:]
		f := join.getField(fkName)
		if f != nil && sqlTypeEqual(pk.SQLType, f.SQLType) {
			fks = append(fks, f)
		}
	}
	if len(fks) > 0 {
		return fks
	}

	if fk == "" || len(base.PKs) != 1 {
		return nil
	}

	if tryFK {
		f := join.getField(fk)
		if f != nil && sqlTypeEqual(base.PKs[0].SQLType, f.SQLType) {
			fks = append(fks, f)
			return fks
		}
	}

	for _, suffix := range []string{"id", "uuid"} {
		f := join.getField(fk + suffix)
		if f != nil && sqlTypeEqual(base.PKs[0].SQLType, f.SQLType) {
			fks = append(fks, f)
			return fks
		}
	}

	return nil
}

func (t *Table) getField(name string) *Field {
	return t.FieldsMap[name]
}

func scanJSONValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(non-pointer %s)", v.Type())
	}
	if b == nil {
		v.Set(reflect.New(v.Type()).Elem())
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	return dec.Decode(v.Addr().Interface())
}

func tryUnderscorePrefix(s string) string {
	if s == "" {
		return s
	}
	if c := s[0]; internal.IsUpper(c) {
		return internal.Underscore(s) + "_"
	}
	return s
}

func quoteTableName(s string) string {
	if isPostgresKeyword(s) {
		return `"` + s + `"`
	}
	return s
}
