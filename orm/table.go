package orm

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/internal/iszero"
	"github.com/go-pg/pg/internal/tag"
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
)

var timeType = reflect.TypeOf((*time.Time)(nil)).Elem()
var nullTimeType = reflect.TypeOf((*types.NullTime)(nil)).Elem()
var ipType = reflect.TypeOf((*net.IP)(nil)).Elem()
var ipNetType = reflect.TypeOf((*net.IPNet)(nil)).Elem()
var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
var nullBoolType = reflect.TypeOf((*sql.NullBool)(nil)).Elem()
var nullFloatType = reflect.TypeOf((*sql.NullFloat64)(nil)).Elem()
var nullIntType = reflect.TypeOf((*sql.NullInt64)(nil)).Elem()
var nullStringType = reflect.TypeOf((*sql.NullString)(nil)).Elem()
var jsonRawMessageType = reflect.TypeOf((*json.RawMessage)(nil)).Elem()

// Table represents a SQL table created from Go struct.
type Table struct {
	Type       reflect.Type
	zeroStruct reflect.Value

	TypeName  string
	Alias     types.Q
	ModelName string

	Name               string
	FullName           types.Q
	FullNameForSelects types.Q

	Tablespace types.Q

	allFields     []*Field // read only
	skippedFields []*Field

	Fields      []*Field // PKs + DataFields
	PKs         []*Field
	DataFields  []*Field
	fieldsMapMu sync.RWMutex
	FieldsMap   map[string]*Field

	Methods   map[string]*Method
	Relations map[string]*Relation
	Unique    map[string][]*Field

	SoftDeleteField *Field

	flags uint16
}

func newTable(typ reflect.Type) *Table {
	t := new(Table)
	t.Type = typ
	t.zeroStruct = reflect.New(t.Type).Elem()
	t.TypeName = internal.ToExported(t.Type.Name())
	t.ModelName = internal.Underscore(t.Type.Name())
	t.Name = tableNameInflector(t.ModelName)
	t.setName(types.Q(types.AppendField(nil, t.Name, 1)))
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

	if typ.Implements(oldAfterQueryHookType) {
		panic(fmt.Sprintf("%s.AfterQuery must be updated - https://github.com/go-pg/pg/wiki/Model-Hooks", t.TypeName))
	}
	if typ.Implements(oldBeforeSelectQueryHookType) {
		panic(fmt.Sprintf("%s.BeforeSelectQuery must be updated - https://github.com/go-pg/pg/wiki/Model-Hooks", t.TypeName))
	}
	if typ.Implements(oldAfterSelectHookType) {
		panic(fmt.Sprintf("%s.AfterSelect must be updated - https://github.com/go-pg/pg/wiki/Model-Hooks", t.TypeName))
	}
	if typ.Implements(oldBeforeInsertHookType) {
		panic(fmt.Sprintf("%s.BeforeInsert must be updated - https://github.com/go-pg/pg/wiki/Model-Hooks", t.TypeName))
	}
	if typ.Implements(oldAfterInsertHookType) {
		panic(fmt.Sprintf("%s.AfterInsert must be updated - https://github.com/go-pg/pg/wiki/Model-Hooks", t.TypeName))
	}
	if typ.Implements(oldBeforeUpdateHookType) {
		panic(fmt.Sprintf("%s.BeforeUpdate must be updated - https://github.com/go-pg/pg/wiki/Model-Hooks", t.TypeName))
	}
	if typ.Implements(oldAfterUpdateHookType) {
		panic(fmt.Sprintf("%s.AfterUpdate must be updated - https://github.com/go-pg/pg/wiki/Model-Hooks", t.TypeName))
	}
	if typ.Implements(oldBeforeDeleteHookType) {
		panic(fmt.Sprintf("%s.BeforeDelete must be updated - https://github.com/go-pg/pg/wiki/Model-Hooks", t.TypeName))
	}
	if typ.Implements(oldAfterDeleteHookType) {
		panic(fmt.Sprintf("%s.AfterDelete must be updated - https://github.com/go-pg/pg/wiki/Model-Hooks", t.TypeName))
	}

	return t
}

func (t *Table) init1() {
	t.initFields()
	t.initMethods()
}

func (t *Table) init2() {
	t.initInlines()
	t.initRelations()
	t.skippedFields = nil
}

func (t *Table) setName(name types.Q) {
	t.FullName = name
	t.FullNameForSelects = name
	if t.Alias == "" {
		t.Alias = name
	}
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

func (t *Table) checkPKs() error {
	if len(t.PKs) == 0 {
		return fmt.Errorf("pg: %s does not have primary keys", t)
	}
	return nil
}

func (t *Table) mustSoftDelete() error {
	if t.SoftDeleteField == nil {
		return fmt.Errorf("pg: %s does not support soft deletes", t)
	}
	return nil
}

func (t *Table) AddField(field *Field) {
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

func (t *Table) getField(name string) *Field {
	t.fieldsMapMu.RLock()
	field := t.FieldsMap[name]
	t.fieldsMapMu.RUnlock()
	return field
}

func (t *Table) HasField(name string) bool {
	_, ok := t.FieldsMap[name]
	return ok
}

func (t *Table) GetField(name string) (*Field, error) {
	field, ok := t.FieldsMap[name]
	if !ok {
		return nil, fmt.Errorf("pg: can't find column=%s in %s", name, t)
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

			pgTag := tag.Parse(f.Tag.Get("pg"))
			_, inherit := pgTag.Options["inherit"]
			_, override := pgTag.Options["override"]
			if inherit || override {
				embeddedTable := _tables.get(fieldType, true)
				t.TypeName = embeddedTable.TypeName
				t.FullName = embeddedTable.FullName
				t.FullNameForSelects = embeddedTable.FullNameForSelects
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
	sqlTag := tag.Parse(f.Tag.Get("sql"))

	switch f.Name {
	case "tableName", "TableName":
		if len(index) > 0 {
			return nil
		}

		tableSpace, ok := sqlTag.Options["tablespace"]
		if ok {
			s, _ := tag.Unquote(tableSpace)
			t.Tablespace = types.Q(internal.QuoteTableName(s))
		}

		if sqlTag.Name == "_" {
			t.setName("")
		} else if sqlTag.Name != "" {
			s, _ := tag.Unquote(sqlTag.Name)
			t.setName(types.Q(internal.QuoteTableName(s)))
		}

		if v, ok := sqlTag.Options["select"]; ok {
			v, _ = tag.Unquote(v)
			t.FullNameForSelects = types.Q(internal.QuoteTableName(v))
		}

		if v, ok := sqlTag.Options["alias"]; ok {
			v, _ = tag.Unquote(v)
			t.Alias = types.Q(internal.QuoteTableName(v))
		}

		pgTag := tag.Parse(f.Tag.Get("pg"))
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
	if field := t.getField(sqlTag.Name); field != nil {
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
		v, ok = tag.Unquote(v)
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
	} else if len(t.PKs) == 0 && !sqlTag.HasOption("nopk") {
		switch field.SQLName {
		case "id", "uuid", "pk_" + t.ModelName:
			field.SetFlag(PrimaryKeyFlag)
		}
	}

	pgTag := tag.Parse(f.Tag.Get("pg"))

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

	if v, ok := sqlTag.Options["on_update"]; ok {
		field.OnUpdate = v
	}

	if _, ok := sqlTag.Options["composite"]; ok {
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
	field.isZero = iszero.Checker(f.Type)

	if v, ok := sqlTag.Options["alias"]; ok {
		v, _ = tag.Unquote(v)
		t.FieldsMap[v] = field
	}

	t.allFields = append(t.allFields, field)
	if skip {
		t.skippedFields = append(t.skippedFields, field)
		t.FieldsMap[field.SQLName] = field
		return nil
	}

	if _, ok := pgTag.Options["soft_delete"]; ok {
		switch field.Type {
		case timeType, nullTimeType:
			t.SoftDeleteField = field
		default:
			err := fmt.Errorf(
				"soft_delete is only supported for time.Time and pg.NullTime")
			panic(err)
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

func (t *Table) initInlines() {
	for _, f := range t.skippedFields {
		if f.Type.Kind() == reflect.Struct {
			t.inlineFields(f, nil)
		}
	}
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

	pgTag := tag.Parse(field.Field.Tag.Get("pg"))
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

	if len(fks) != len(fkValues) {
		panic("len(fks) != len(fkValues)")
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
	pgTag := tag.Parse(field.Field.Tag.Get("pg"))
	joinTable := _tables.get(field.Type, true)
	if len(joinTable.allFields) == 0 {
		return false
	}

	res := t.tryHasOne(joinTable, field, pgTag) ||
		t.tryBelongsToOne(joinTable, field, pgTag)
	t.inlineFields(field, nil)
	return res
}

func (t *Table) inlineFields(strct *Field, path map[reflect.Type]struct{}) {
	if path == nil {
		path = map[reflect.Type]struct{}{
			t.Type: struct{}{},
		}
	}

	if _, ok := path[strct.Type]; ok {
		return
	}
	path[strct.Type] = struct{}{}

	joinTable := _tables.get(strct.Type, true)
	for _, f := range joinTable.allFields {
		f = f.Copy()
		f.GoName = strct.GoName + "_" + f.GoName
		f.SQLName = strct.SQLName + "__" + f.SQLName
		f.Column = types.Q(types.AppendField(nil, f.SQLName, 1))
		f.Index = appendNew(strct.Index, f.Index...)

		t.fieldsMapMu.Lock()
		if _, ok := t.FieldsMap[f.SQLName]; !ok {
			t.FieldsMap[f.SQLName] = f
		}
		t.fieldsMapMu.Unlock()

		if f.Type.Kind() != reflect.Struct {
			continue
		}

		if _, ok := path[f.Type]; !ok {
			t.inlineFields(f, path)
		}
	}
}

func appendNew(dst []int, src ...int) []int {
	cp := make([]int, len(dst)+len(src))
	copy(cp, dst)
	copy(cp[len(dst):], src)
	return cp
}

func isColumn(typ reflect.Type) bool {
	return typ.Implements(scannerType) || reflect.PtrTo(typ).Implements(scannerType)
}

func fieldSQLType(field *Field, pgTag, sqlTag *tag.Tag) string {
	if typ, ok := sqlTag.Options["type"]; ok {
		field.SetFlag(customTypeFlag)
		typ, _ = tag.Unquote(typ)
		return typ
	}

	if typ, ok := sqlTag.Options["composite"]; ok {
		field.SetFlag(customTypeFlag)
		typ, _ = tag.Unquote(typ)
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
		switch field.Type.Kind() {
		case reflect.Slice, reflect.Array:
			sqlType := sqlType(field.Type.Elem())
			return sqlType + "[]"
		}
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
	case jsonRawMessageType:
		return "jsonb"
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

func (t *Table) tryHasOne(joinTable *Table, field *Field, pgTag *tag.Tag) bool {
	fk, fkOK := pgTag.Options["fk"]
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

func (t *Table) tryBelongsToOne(joinTable *Table, field *Field, pgTag *tag.Tag) bool {
	fk, fkOK := pgTag.Options["fk"]
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
			continue
		}

		if strings.IndexByte(pk.SQLName, '_') == -1 {
			continue
		}

		f = join.getField(pk.SQLName)
		if f != nil && sqlTypeEqual(pk.SQLType, f.SQLType) {
			fks = append(fks, f)
			continue
		}
	}
	if len(fks) > 0 && len(fks) == len(base.PKs) {
		return fks
	}

	fks = nil
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
	if len(fks) > 0 && len(fks) == len(base.PKs) {
		return fks
	}

	if fk == "" || len(base.PKs) != 1 {
		return nil
	}

	if tryFK {
		f := join.getField(fk)
		if f != nil && sqlTypeEqual(base.PKs[0].SQLType, f.SQLType) {
			return []*Field{f}
		}
	}

	for _, suffix := range []string{"id", "uuid"} {
		f := join.getField(fk + suffix)
		if f != nil && sqlTypeEqual(base.PKs[0].SQLType, f.SQLType) {
			return []*Field{f}
		}
	}

	return nil
}

func scanJSONValue(v reflect.Value, rd types.Reader, n int) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(non-pointer %s)", v.Type())
	}

	if n == -1 {
		v.Set(reflect.New(v.Type()).Elem())
		return nil
	}

	dec := json.NewDecoder(rd)
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
