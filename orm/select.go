package orm

import (
	"fmt"
	"strconv"
)

func Select(db DB, model interface{}) error {
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
	if len(sel.with) > 0 {
		b = append(b, "WITH "...)
		b = append(b, sel.with...)
		b = append(b, ' ')
	}

	b = append(b, "SELECT "...)
	if sel.columns == nil {
		b = sel.appendColumns(b)
	} else {
		b = append(b, sel.columns...)
	}

	if sel.haveTables() {
		b = append(b, " FROM "...)
		b = sel.appendTables(b)
	}

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

func (sel selectQuery) appendColumns(b []byte) []byte {
	if sel.model != nil {
		return sel.appendModelColumns(b)
	}

	var ok bool
	b, ok = sel.appendTableAlias(b)
	if ok {
		b = append(b, '.')
	}
	b = append(b, '*')
	return b
}

func (sel selectQuery) appendModelColumns(b []byte) []byte {
	alias, hasAlias := sel.appendTableAlias(nil)
	for i, f := range sel.model.Table().Fields {
		if i > 0 {
			b = append(b, ", "...)
		}
		if hasAlias {
			b = append(b, alias...)
			b = append(b, '.')
		}
		b = append(b, f.ColName...)
	}
	return b
}
