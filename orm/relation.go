package orm

import (
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/pg.v3/types"
)

type Join struct {
	BaseModel, JoinModel *Model
	TableRelation        *TableRelation
	Columns              []string
}

func (j *Join) AppendColumns(columns []string) []string {
	alias := j.TableRelation.Field.SQLName
	prefix := alias + "__"
	if j.Columns != nil {
		for _, column := range j.Columns {
			column = fmt.Sprintf("%s.%s AS %s", alias, column, prefix+column)
			columns = append(columns, column)
		}
		return columns
	}
	for _, f := range j.JoinModel.Table.Fields {
		column := fmt.Sprintf("%s.%s AS %s", alias, f.SQLName, prefix+f.SQLName)
		columns = append(columns, column)
	}
	return columns
}

func (j *Join) JoinOne(s *Select) *Select {
	s = s.Table(j.JoinModel.Table.Name + " AS " + j.TableRelation.Field.SQLName)
	s.columns = j.AppendColumns(s.columns)
	s = s.Where(
		`?."id" = ?.?`,
		types.F(j.TableRelation.Field.SQLName),
		types.F(j.BaseModel.Table.Name),
		types.F(j.TableRelation.Field.SQLName+"_id"),
	)
	return s
}

func (h *Join) JoinMany(db dber, bind reflect.Value) error {
	path := h.JoinModel.Path[:len(h.JoinModel.Path)-1]

	pk := h.appendPK(nil, bind, path)
	if pk != nil {
		pk = pk[:len(pk)-2] // trim ", "
	}

	joinSlicePtr := reflect.New(reflect.SliceOf(h.JoinModel.Table.Type))
	err := NewSelect(db).Where(
		`?.? IN (?)`,
		types.F(h.JoinModel.Table.Name), types.F(h.BaseModel.Table.Name+"_id"), types.Q(pk),
	).Find(joinSlicePtr).Err()
	if err != nil {
		return err
	}

	h.assignValues(bind, joinSlicePtr.Elem(), path)

	return nil
}

func (h *Join) appendPK(b []byte, v reflect.Value, path []string) []byte {
	if v.Kind() == reflect.Slice {
		return h.appendPKSlice(b, v, path)
	} else {
		return h.appendPKStruct(b, v, path)
	}
}

func (h *Join) appendPKSlice(b []byte, slice reflect.Value, path []string) []byte {
	for i := 0; i < slice.Len(); i++ {
		b = h.appendPKStruct(b, slice.Index(i), path)
	}
	return b
}

func (h *Join) appendPKStruct(b []byte, strct reflect.Value, path []string) []byte {
	if len(path) > 0 {
		strct = strct.FieldByName(path[0])
		b = h.appendPKSlice(b, strct, path[1:])
	} else {
		b = h.BaseModel.Table.PK.AppendValue(b, strct, true)
		b = append(b, ", "...)
	}
	return b
}

func (h *Join) assignValues(base, joinSlice reflect.Value, path []string) {
	if base.Kind() == reflect.Slice {
		h.assignValuesSlice(base, joinSlice, path)
	} else {
		h.assignValuesStruct(base, joinSlice, path)
	}
}

func (h *Join) assignValuesSlice(baseSlice, joinSlice reflect.Value, path []string) {
	for i := 0; i < baseSlice.Len(); i++ {
		h.assignValuesStruct(baseSlice.Index(i), joinSlice, path)
	}
}

func (h *Join) assignValuesStruct(baseStruct, joinSlice reflect.Value, path []string) {
	if len(path) > 0 {
		v := baseStruct.FieldByName(path[0])
		h.assignValues(v, joinSlice, path[1:])
	} else {
		hasManySlice := h.TableRelation.Field.Value(baseStruct)
		for j := 0; j < joinSlice.Len(); j++ {
			join := joinSlice.Index(j)
			if h.equal(baseStruct, join) {
				hasManySlice.Set(reflect.Append(hasManySlice, join))
			}
		}
	}
}

func (h *Join) equal(base, join reflect.Value) bool {
	field := h.JoinModel.Table.PK.GoName                          // Id
	v1 := base.FieldByName(field)                                 // BaseTable.Id
	v2 := join.FieldByName(h.BaseModel.Table.Type.Name() + field) // JoinTable.BaseId
	return v1.Int() == v2.Int()
}

type Relation struct {
	Model *Model
	Joins []Join
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
	var goPath []string

	baseModel := rel.Model
	joinModel := baseModel
	var tableRel *TableRelation
	var retErr error

	for _, name := range path {
		if v, ok := joinModel.Table.Relations[name]; ok {
			tableRel = v
			goPath = append(goPath, tableRel.Field.GoName)

			if v, ok := rel.getJoin(name); ok {
				baseModel = joinModel
				joinModel = v.JoinModel
				continue
			}

			model, err := NewModelPath(rel.Model.Value(), goPath, tableRel.Join)
			if err != nil {
				retErr = err
				break
			}

			baseModel = joinModel
			joinModel = model
			continue
		}

		retErr = fmt.Errorf("pg: %s doesn't have %s relation", baseModel.Table.Name, name)
		break
	}

	if joinModel == baseModel {
		return retErr
	}

	var columns []string
	switch len(path) - len(goPath) {
	case 0:
		// ok
	case 1:
		columns = path[len(path)-1:]
	default:
		return fmt.Errorf("pg: bad column name: %s", name)
	}

	rel.Joins = append(rel.Joins, Join{
		BaseModel:     baseModel,
		JoinModel:     joinModel,
		TableRelation: tableRel,
		Columns:       columns,
	})

	return nil
}

func (rel *Relation) getJoin(name string) (join Join, ok bool) {
	for _, join = range rel.Joins {
		if join.TableRelation.Field.SQLName == name {
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
	for _, join := range rel.Joins {
		if !join.TableRelation.Many {
			join.JoinModel.Bind(rel.Model.strct)
		}
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
	join, ok := rel.getJoin(modelName)
	if ok {
		return join.JoinModel.ScanColumn(colIdx, colName, b)
	}
	return rel.Model.ScanColumn(colIdx, colName, b)
}
