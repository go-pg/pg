package orm

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-pg/pg/types"
)

type m2mModel struct {
	*sliceTableModel
	baseTable *Table
	rel       *Relation

	buf       []byte
	dstValues map[string][]reflect.Value
	columns   map[string]string
}

var _ TableModel = (*m2mModel)(nil)

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

func (m *m2mModel) AfterQuery(c context.Context, db DB) error {
	if m.rel.JoinTable.HasFlag(AfterQueryHookFlag) {
		var firstErr error
		for _, slices := range m.dstValues {
			for _, slice := range slices {
				err := callAfterQueryHookSlice(slice, m.sliceOfPtr, c, db)
				if err != nil && firstErr == nil {
					firstErr = err
				}
			}
		}
		return firstErr
	}

	return nil
}

func (m *m2mModel) AfterSelect(c context.Context, db DB) error {
	return nil
}

func (m *m2mModel) BeforeInsert(c context.Context, db DB) error {
	return nil
}

func (m *m2mModel) AfterInsert(c context.Context, db DB) error {
	return nil
}

func (m *m2mModel) BeforeUpdate(c context.Context, db DB) error {
	return nil
}

func (m *m2mModel) AfterUpdate(c context.Context, db DB) error {
	return nil
}

func (m *m2mModel) BeforeDelete(c context.Context, db DB) error {
	return nil
}

func (m *m2mModel) AfterDelete(c context.Context, db DB) error {
	return nil
}

func (m *m2mModel) ScanColumn(colIdx int, colName string, rd types.Reader, n int) error {
	ok, err := m.sliceTableModel.scanColumn(colIdx, colName, rd, n)
	if ok {
		return err
	}

	tmp, err := rd.ReadFullTemp()
	if err != nil {
		return err
	}

	m.columns[colName] = string(tmp)
	return nil
}
