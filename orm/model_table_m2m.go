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

func newM2MModel(j *join) *m2mModel {
	baseTable := j.BaseModel.Table()
	joinModel := j.JoinModel.(*sliceTableModel)
	dstValues := dstValues(joinModel, baseTable.PKs)
	if len(dstValues) == 0 {
		return nil
	}
	m := &m2mModel{
		sliceTableModel: joinModel,
		baseTable:       baseTable,
		rel:             j.Rel,

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
	m.structInited = false
	m.structTableModel.NewModel()
	return m
}

func (m *m2mModel) AddModel(model ColumnScanner) error {
	m.buf = modelIdMap(m.buf[:0], m.columns, m.rel.BaseFKs)
	dstValues, ok := m.dstValues[string(m.buf)]
	if !ok {
		return fmt.Errorf(
			"pg: relation=%q has no base %s with id=%q (check join conditions)",
			m.rel.Field.GoName, m.baseTable, m.buf)
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

func modelIdMap(b []byte, m map[string]string, columns []string) []byte {
	for i, col := range columns {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, m[col]...)
	}
	return b
}

func (m *m2mModel) AfterQuery(db DB) error {
	if !m.rel.JoinTable.HasFlag(AfterQueryHookFlag) {
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
