package orm

import (
	"fmt"
	"reflect"
)

type manyModel struct {
	*sliceTableModel
	rel *Relation

	buf        []byte
	zeroStruct reflect.Value
	dstValues  map[string][]reflect.Value
}

var _ tableModel = (*manyModel)(nil)

func newManyModel(j *join) *manyModel {
	joinModel := j.JoinModel.(*sliceTableModel)
	dstValues := dstValues(joinModel.Root(), joinModel.Index(), j.BaseModel.Table().PKs)
	m := manyModel{
		sliceTableModel: joinModel,
		rel:             j.Rel,

		dstValues: dstValues,
	}
	if !m.sliceOfPtr {
		m.strct = reflect.New(m.table.Type).Elem()
		m.zeroStruct = reflect.Zero(m.table.Type)
	}
	return &m
}

func (m *manyModel) NewModel() ColumnScanner {
	if m.sliceOfPtr {
		m.strct = reflect.New(m.table.Type).Elem()
	} else {
		m.strct.Set(m.zeroStruct)
	}
	m.structTableModel.NewModel()
	return m
}

func (m *manyModel) AddModel(_ ColumnScanner) error {
	m.buf = modelId(m.buf[:0], m.strct, m.rel.FKs)
	dstValues, ok := m.dstValues[string(m.buf)]
	if !ok {
		return fmt.Errorf("pg: can't find dst value for model id=%q", m.buf)
	}

	if m.sliceOfPtr {
		for _, v := range dstValues {
			v.Set(reflect.Append(v, m.strct.Addr()))
		}
	} else {
		for _, v := range dstValues {
			v.Set(reflect.Append(v, m.strct))
		}
	}

	return nil
}
