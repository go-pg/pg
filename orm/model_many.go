package orm

import (
	"fmt"
	"reflect"
)

type manyModel struct {
	*sliceTableModel
	rel *Relation

	dstValues map[string][]reflect.Value
}

var _ tableModel = (*manyModel)(nil)

func newManyModel(join *Join) *manyModel {
	joinModel := join.JoinModel.(*sliceTableModel)
	dstValues := dstValues(joinModel.Root(), joinModel.Path(), join.BaseModel.Table().PKs)
	return &manyModel{
		sliceTableModel: joinModel,
		rel:             join.Rel,

		dstValues: dstValues,
	}
}

func (m *manyModel) NewModel() ColumnScanner {
	m.strct = reflect.New(m.table.Type).Elem()
	m.structTableModel.NewModel()
	return m
}

func (m *manyModel) AddModel(_ ColumnScanner) error {
	id := string(modelId(nil, m.strct, m.rel.FKs))
	dstValues, ok := m.dstValues[id]
	if !ok {
		return fmt.Errorf("pg: can't find dst value for model id=%q", id)
	}
	for _, v := range dstValues {
		v.Set(reflect.Append(v, m.strct))
	}
	return nil
}
