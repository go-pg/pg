package orm

import (
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/pg.v3/types"
)

type HasOne struct {
	Base, Join *Model
	Field      *Field
}

func (h *HasOne) Do(s *Select) *Select {
	s = s.Table(h.Join.Table.Name)
	s.columns = h.Join.Columns(s.columns, h.Join.Table.Name+"__")
	s = s.Where(
		`?."id" = ?.?`,
		types.F(h.Join.Table.Name),
		types.F(h.Base.Table.Name),
		types.F(h.Field.SQLName+"_id"),
	)
	return s
}

type HasMany struct {
	Base, Join *Model
	Field      *Field
}

func (h *HasMany) Do(db dber, bind reflect.Value) error {
	path := h.Join.Path[:len(h.Join.Path)-1]

	pk := h.appendPK(nil, bind, path)
	if pk != nil {
		pk = pk[:len(pk)-2] // trim ", "
	}

	joinSlicePtr := reflect.New(reflect.SliceOf(h.Join.Table.Type))
	err := NewSelect(db).Where(
		`?.? IN (?)`,
		types.F(h.Join.Table.Name), types.F(h.Base.Table.Name+"_id"), types.Q(pk),
	).Find(joinSlicePtr).Err()
	if err != nil {
		return err
	}

	h.assignValues(bind, joinSlicePtr.Elem(), path)

	return nil
}

func (h *HasMany) appendPK(b []byte, v reflect.Value, path []string) []byte {
	if v.Kind() == reflect.Slice {
		return h.appendPKSlice(b, v, path)
	} else {
		return h.appendPKStruct(b, v, path)
	}
}

func (h *HasMany) appendPKSlice(b []byte, slice reflect.Value, path []string) []byte {
	for i := 0; i < slice.Len(); i++ {
		b = h.appendPKStruct(b, slice.Index(i), path)
	}
	return b
}

func (h *HasMany) appendPKStruct(b []byte, strct reflect.Value, path []string) []byte {
	if len(path) > 0 {
		strct = strct.FieldByName(path[0])
		b = h.appendPKSlice(b, strct, path[1:])
	} else {
		b = h.Base.Table.PK.AppendValue(b, strct, true)
		b = append(b, ", "...)
	}
	return b
}

func (h *HasMany) assignValues(base, joinSlice reflect.Value, path []string) {
	if base.Kind() == reflect.Slice {
		h.assignValuesSlice(base, joinSlice, path)
	} else {
		h.assignValuesStruct(base, joinSlice, path)
	}
}

func (h *HasMany) assignValuesSlice(baseSlice, joinSlice reflect.Value, path []string) {
	for i := 0; i < baseSlice.Len(); i++ {
		h.assignValuesStruct(baseSlice.Index(i), joinSlice, path)
	}
}

func (h *HasMany) assignValuesStruct(baseStruct, joinSlice reflect.Value, path []string) {
	if len(path) > 0 {
		v := baseStruct.FieldByName(path[0])
		h.assignValues(v, joinSlice, path[1:])
	} else {
		hasManySlice := h.Field.Value(baseStruct)
		for j := 0; j < joinSlice.Len(); j++ {
			join := joinSlice.Index(j)
			if h.equal(baseStruct, join) {
				hasManySlice.Set(reflect.Append(hasManySlice, join))
			}
		}
	}
}

func (h *HasMany) equal(base, join reflect.Value) bool {
	field := h.Join.Table.PK.GoName                          // Id
	v1 := base.FieldByName(field)                            // Base.Id
	v2 := join.FieldByName(h.Base.Table.Type.Name() + field) // Join.BaseId
	return v1.Int() == v2.Int()
}

type Relation struct {
	Model   *Model
	HasOne  map[string]HasOne
	HasMany []HasMany // list because order is important
}

var (
	_ Collection    = (*Relation)(nil)
	_ ColumnScanner = (*Relation)(nil)
)

func NewRelation(vi interface{}) (*Relation, error) {
	switch v := vi.(type) {
	case *Relation:
		return v, nil
	case *Model:
		return &Relation{
			Model: v,
		}, nil
	default:
		model, err := NewModel(vi)
		if err != nil {
			return nil, err
		}
		return &Relation{
			Model: model,
		}, nil
	}
}

func (rel *Relation) AddRelation(name string) error {
	path := strings.Split(name, ".")

	base := rel.Model
	var goPath []string

	for _, name := range path {
		if field, ok := base.Table.HasOne[name]; ok {
			goPath = append(goPath, field.GoName)
			if hasOne, ok := rel.HasOne[name]; ok {
				base = hasOne.Join
				continue
			}
			join, err := NewModelPath(rel.Model.Value(), goPath)
			if err != nil {
				return err
			}
			rel.hasOne(name, HasOne{
				Base:  base,
				Join:  join,
				Field: field,
			})
			base = join
			continue
		}

		if field, ok := base.Table.HasMany[name]; ok {
			goPath = append(goPath, field.GoName)
			if hasMany, ok := rel.getHasMany(name); ok {
				base = hasMany.Join
				continue
			}
			join, err := NewModelPath(rel.Model.Value(), goPath)
			if err != nil {
				return err
			}
			rel.HasMany = append(rel.HasMany, HasMany{
				Base:  base,
				Join:  join,
				Field: field,
			})
			base = join
			continue
		}

		return fmt.Errorf("pg: %s doesn't have %s relation", base.Table.Name, name)
	}

	return nil
}

func (rel *Relation) hasOne(name string, one HasOne) {
	if rel.HasOne == nil {
		rel.HasOne = make(map[string]HasOne)
	}
	rel.HasOne[name] = one
}

func (rel *Relation) getHasMany(name string) (hasMany HasMany, ok bool) {
	for _, hasMany = range rel.HasMany {
		if hasMany.Field.SQLName == name {
			ok = true
			return
		}
	}
	return
}

func (rel *Relation) AppendParam(b []byte, name string) ([]byte, error) {
	return rel.Model.AppendParam(b, name)
}

func (rel *Relation) NextModel() interface{} {
	rel.Model.NextModel()
	for _, hasOne := range rel.HasOne {
		hasOne.Join.Bind(rel.Model.strct)
	}
	return rel
}

func splitColumn(s string) (string, string) {
	parts := strings.SplitN(s, "__", 2)
	if len(parts) != 2 {
		return "", s
	}
	return parts[0], parts[1]
}

func (rel *Relation) ScanColumn(colIdx int, colName string, b []byte) error {
	modelName, colName := splitColumn(colName)
	hasOne, ok := rel.HasOne[modelName]
	if ok {
		return hasOne.Join.ScanColumn(colIdx, colName, b)
	}
	return rel.Model.ScanColumn(colIdx, colName, b)
}
