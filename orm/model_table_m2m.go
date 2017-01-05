package orm

import (
	"fmt"
	"reflect"
)

type m2mModel struct {
	*sliceTableModel
	baseTable *Table
	rel       *Relation

	buf       []byte
	dstValues map[string][]reflect.Value
	columns   map[string]string
}

var _ tableModel = (*m2mModel)(nil)

func newM2MModel(join *join) *m2mModel {
	baseTable := join.BaseModel.Table()
	joinModel := join.JoinModel.(*sliceTableModel)
	dstValues := dstValues(joinModel, baseTable.PKs)
	m := &m2mModel{
		sliceTableModel: joinModel,
		baseTable:       baseTable,
		rel:             join.Rel,

		dstValues: dstValues,
		columns:   make(map[string]string),
	}
	if !m.sliceOfPtr {
		m.strct = reflect.New(m.table.Type).Elem()
	}
	return m
}

func (m *m2mModel) NewModel() ColumnScanner {
	if m.sliceOfPtr {
		m.strct = reflect.New(m.table.Type).Elem()
	} else {
		m.strct.Set(m.table.zeroStruct)
	}
	m.structTableModel.NewModel()
	return m
}

func (m *m2mModel) AddModel(model ColumnScanner) error {
	m.buf = modelIdMap(m.buf[:0], m.columns, m.baseTable.ModelName+"_", m.baseTable.PKs)
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

func (m *m2mModel) AfterQuery(db DB) error {
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

func (m *m2mModel) AfterSelect(db DB) error {
	return nil
}

func (m *m2mModel) BeforeInsert(db DB) error {
	return nil
}

func (m *m2mModel) AfterInsert(db DB) error {
	return nil
}

func (m *m2mModel) BeforeUpdate(db DB) error {
	return nil
}

func (m *m2mModel) AfterUpdate(db DB) error {
	return nil
}

func (m *m2mModel) BeforeDelete(db DB) error {
	return nil
}

func (m *m2mModel) AfterDelete(db DB) error {
	return nil
}

func (m *m2mModel) ScanColumn(colIdx int, colName string, b []byte) error {
	ok, err := m.sliceTableModel.scanColumn(colIdx, colName, b)
	if ok {
		return err
	}

	m.columns[colName] = string(b)
	return nil
}
