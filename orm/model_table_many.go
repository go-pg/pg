package orm

import (
	"fmt"
	"reflect"
)

type manyModel struct {
	*sliceTableModel
	rel *Relation

	buf       []byte
	dstValues map[string][]reflect.Value
}

var _ tableModel = (*manyModel)(nil)

func newManyModel(j *join) *manyModel {
	joinModel := j.JoinModel.(*sliceTableModel)
	dstValues := dstValues(joinModel, j.BaseModel.Table().PKs)
	m := manyModel{
		sliceTableModel: joinModel,
		rel:             j.Rel,

		dstValues: dstValues,
	}
	if !m.sliceOfPtr {
		m.strct = reflect.New(m.table.Type).Elem()
	}
	return &m
}

func (m *manyModel) NewModel() ColumnScanner {
	if m.sliceOfPtr {
		m.strct = reflect.New(m.table.Type).Elem()
	} else {
		m.strct.Set(m.table.zeroStruct)
	}
	m.structTableModel.NewModel()
	return m
}

func (m *manyModel) AddModel(model ColumnScanner) error {
	m.buf = modelId(m.buf[:0], m.strct, m.rel.FKs)
	dstValues, ok := m.dstValues[string(m.buf)]
	if !ok {
		return fmt.Errorf("pg: can't find dst value for model id=%q", m.buf)
	}

	for _, v := range dstValues {
		if m.sliceOfPtr {
			v.Set(reflect.Append(v, m.strct.Addr()))
		} else {
			v.Set(reflect.Append(v, m.strct))
		}
	}

	return nil
}

func (m *manyModel) AfterQuery(db DB) error {
	if !m.rel.JoinTable.Has(AfterQueryHookFlag) {
		return nil
	}

	var retErr error
	for _, slices := range m.dstValues {
		for _, slice := range slices {
			err := callAfterQueryHookSlice(slice, m.sliceOfPtr, db)
			if err != nil && retErr == nil {
				retErr = err
			}
		}
	}
	return retErr
}

func (m *manyModel) AfterSelect(db DB) error {
	return nil
}

func (m *manyModel) BeforeInsert(db DB) error {
	return nil
}

func (m *manyModel) AfterInsert(db DB) error {
	return nil
}

func (m *manyModel) BeforeUpdate(db DB) error {
	return nil
}

func (m *manyModel) AfterUpdate(db DB) error {
	return nil
}

func (m *manyModel) BeforeDelete(db DB) error {
	return nil
}

func (m *manyModel) AfterDelete(db DB) error {
	return nil
}
