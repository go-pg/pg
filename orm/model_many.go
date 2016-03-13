package orm

import (
	"fmt"
	"reflect"
)

type manyModel struct {
	*SliceModel
	Rel *Relation

	dstValues map[string][]reflect.Value
}

var _ TableModel = (*manyModel)(nil)

func newManyModel(join *Join) *manyModel {
	return &manyModel{
		SliceModel: join.JoinModel.(*SliceModel),
		Rel:        join.Rel,

		dstValues: dstValues(join.JoinModel),
	}
}

func (m *manyModel) NewModel() ColumnScanner {
	m.strct = reflect.New(m.table.Type).Elem()
	m.StructModel.NewModel()
	return m
}

func (m *manyModel) AddModel(_ ColumnScanner) error {
	id := string(modelId(nil, m.strct, m.Rel.FKs))
	dstValues, ok := m.dstValues[id]
	if !ok {
		return fmt.Errorf("pg: can't find dst value for model id=%q", id)
	}
	for _, v := range dstValues {
		v.Set(reflect.Append(v, m.strct))
	}
	return nil
}
