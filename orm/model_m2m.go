package orm

import (
	"fmt"
	"reflect"
)

type m2mModel struct {
	*SliceModel
	Rel *Relation

	dstValues map[string][]reflect.Value
	columns   map[string]string
}

var _ TableModel = (*m2mModel)(nil)

func newM2MModel(join *Join) *m2mModel {
	return &m2mModel{
		SliceModel: join.JoinModel.(*SliceModel),
		Rel:        join.Rel,

		dstValues: dstValues(join.JoinModel),
		columns:   make(map[string]string),
	}
}

func dstValues(model TableModel) map[string][]reflect.Value {
	path := model.Path()
	mp := make(map[string][]reflect.Value)
	b := make([]byte, 16)
	walk(model.Root(), path[:len(path)-1], func(v reflect.Value) {
		b = b[:0]
		id := string(modelId(b, v, model.Table().PKs))
		mp[id] = append(mp[id], v.FieldByName(path[len(path)-1]))
	})
	return mp
}

func (m *m2mModel) NewModel() ColumnScanner {
	m.strct = reflect.New(m.table.Type).Elem()
	m.StructModel.NewModel()
	return m
}

func (m *m2mModel) AddModel(_ ColumnScanner) error {
	var id []byte
	for _, fk := range m.Rel.M2MBaseFKs {
		s, ok := m.columns[fk.SQLName]
		if !ok {
			return fmt.Errorf("pg: can't find fk=%s", fk.SQLName)
		}
		id = append(id, s...)
	}

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
