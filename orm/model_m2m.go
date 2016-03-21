package orm

import (
	"fmt"
	"reflect"
)

type m2mModel struct {
	*SliceModel
	rel *Relation

	dstValues map[string][]reflect.Value
	columns   map[string]string
}

var _ TableModel = (*m2mModel)(nil)

func newM2MModel(join *Join) *m2mModel {
	return &m2mModel{
		SliceModel: join.JoinModel.(*SliceModel),
		rel:        join.Rel,

		dstValues: dstValues(join.JoinModel),
		columns:   make(map[string]string),
	}
}

func (m *m2mModel) NewModel() ColumnScanner {
	m.strct = reflect.New(m.table.Type).Elem()
	m.StructModel.NewModel()
	return m
}

func (m *m2mModel) AddModel(_ ColumnScanner) error {
	id := modelIdMap(nil, m.columns, m.rel.M2MBaseFKs)
	dstValues, ok := m.dstValues[string(id)]
	if !ok {
		return fmt.Errorf("pg: can't find dst value for model with id=%q", string(id))
	}
	for _, v := range dstValues {
		v.Set(reflect.Append(v, m.strct))
	}
	return nil
}

func (m *m2mModel) ScanColumn(colIdx int, colName string, b []byte) error {
	ok, err := m.SliceModel.scanColumn(colIdx, colName, b)
	if ok {
		return err
	}

	m.columns[colName] = string(b)
	return nil
}
