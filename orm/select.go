package orm

import (
	"fmt"
	"strconv"

	"gopkg.in/pg.v4/types"
)

func Select(db dber, model interface{}) error {
	q := NewQuery(db, model)
	m, ok := q.model.(*structTableModel)
	if !ok {
		return fmt.Errorf("Select expects struct, got %T", model)
	}
	if err := m.table.checkPKs(); err != nil {
		return err
	}
	q.where = appendColumnAndValue(q.where, m.strct, m.table, m.table.PKs)
	return q.Select()
}

type selectQuery struct {
	*Query
}

var _ QueryAppender = (*selectQuery)(nil)

func (sel selectQuery) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	table := sel.model.Table()

	b = append(b, "SELECT "...)
	if sel.columns == nil {
		b = types.AppendField(b, table.ModelName, 1)
		b = append(b, ".*"...)
	} else {
		b = append(b, sel.columns...)
	}

	b = append(b, " FROM "...)
	b = append(b, sel.tables...)

	if len(sel.join) > 0 {
		b = append(b, ' ')
		b = append(b, sel.join...)
	}

	if len(sel.where) > 0 {
		b = append(b, " WHERE "...)
		b = append(b, sel.where...)
	}

	if len(sel.group) > 0 {
		b = append(b, " GROUP BY "...)
		b = append(b, sel.group...)
	}

	if len(sel.order) > 0 {
		b = append(b, " ORDER BY "...)
		b = append(b, sel.order...)
	}

	if sel.limit != 0 {
		b = append(b, " LIMIT "...)
		b = strconv.AppendInt(b, int64(sel.limit), 10)
	}

	if sel.offset != 0 {
		b = append(b, " OFFSET "...)
		b = strconv.AppendInt(b, int64(sel.offset), 10)
	}

	return b, nil
}
