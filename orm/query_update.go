package orm

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/go-pg/pg/v11/types"
)

type UpdateQuery struct {
	q        *Query
	omitZero bool
}

var (
	_ QueryAppender = (*UpdateQuery)(nil)
	_ QueryCommand  = (*UpdateQuery)(nil)
)

func NewUpdateQuery(q *Query, omitZero bool) *UpdateQuery {
	return &UpdateQuery{
		q:        q,
		omitZero: omitZero,
	}
}

func (q *UpdateQuery) String() string {
	b, err := q.AppendQuery(defaultFmter, nil)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func (q *UpdateQuery) Operation() QueryOp {
	return UpdateOp
}

func (q *UpdateQuery) Clone() QueryCommand {
	return &UpdateQuery{
		q:        q.q.Clone(),
		omitZero: q.omitZero,
	}
}

func (q *UpdateQuery) Query() *Query {
	return q.q
}

func (q *UpdateQuery) AppendTemplate(b []byte) ([]byte, error) {
	return q.AppendQuery(dummyFormatter{}, b)
}

func (q *UpdateQuery) AppendQuery(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}

	if len(q.q.with) > 0 {
		b, err = q.q.appendWith(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	b = append(b, "UPDATE "...)

	b, err = q.q.appendFirstTableWithAlias(fmter, b)
	if err != nil {
		return nil, err
	}

	b, err = q.mustAppendSet(fmter, b)
	if err != nil {
		return nil, err
	}

	b, err = q.appendOtherTables(fmter, b)
	if err != nil {
		return nil, err
	}

	b, err = q.q.mustAppendWhere(fmter, b)
	if err != nil {
		return nil, err
	}

	if len(q.q.returning) > 0 {
		b, err = q.q.appendReturning(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	return b, q.q.stickyErr
}

func (q *UpdateQuery) mustAppendSet(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	if len(q.q.set) > 0 {
		return q.q.appendSet(fmter, b)
	}

	b = append(b, " SET "...)

	if m, ok := q.q.model.(*mapModel); ok {
		return q.appendMapSet(fmter, b, m.m), nil
	}

	if !q.q.hasTableModel() {
		return nil, errModelNil
	}

	value := q.q.tableModel.Value()
	if value.Kind() == reflect.Struct {
		b, err = q.appendSetStruct(fmter, b, value)
	} else {
		if value.Len() > 0 {
			b, err = q.appendSetSlice(b)
		} else {
			err = fmt.Errorf("pg: can't bulk-update empty slice %s", value.Type())
		}
	}
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (q *UpdateQuery) appendMapSet(fmter QueryFormatter, b []byte, m map[string]interface{}) []byte {
	keys := make([]string, 0, len(m))

	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	isTemplate := isTemplateFormatter(fmter)
	for i, k := range keys {
		if i > 0 {
			b = append(b, ", "...)
		}

		b = types.AppendIdent(b, k, 1)
		b = append(b, " = "...)
		if isTemplate {
			b = append(b, '?')
		} else {
			b = types.Append(b, m[k], 1)
		}
	}

	return b
}

func (q *UpdateQuery) appendSetStruct(fmter QueryFormatter, b []byte, strct reflect.Value) ([]byte, error) {
	fields, err := q.q.getDataFields()
	if err != nil {
		return nil, err
	}

	isTemplate := isTemplateFormatter(fmter)
	pos := len(b)
	for _, f := range fields {
		if q.omitZero && f.NullZero() && f.HasZeroValue(strct) {
			continue
		}

		if len(b) != pos {
			b = append(b, ", "...)
			pos = len(b)
		}

		b = append(b, f.Column...)
		b = append(b, " = "...)

		if isTemplate {
			b = append(b, '?')
			continue
		}

		app, ok := q.q.modelValues[f.SQLName]
		if ok {
			b, err = app.AppendQuery(fmter, b)
			if err != nil {
				return nil, err
			}
		} else {
			b = f.AppendValue(b, strct, 1)
		}
	}

	for i, v := range q.q.extraValues {
		if i > 0 || len(fields) > 0 {
			b = append(b, ", "...)
		}

		b = append(b, v.column...)
		b = append(b, " = "...)

		b, err = v.value.AppendQuery(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

func (q *UpdateQuery) appendSetSlice(b []byte) ([]byte, error) {
	fields, err := q.q.getDataFields()
	if err != nil {
		return nil, err
	}

	var table *Table
	if q.omitZero {
		table = q.q.tableModel.Table()
	}

	for i, f := range fields {
		if i > 0 {
			b = append(b, ", "...)
		}

		b = append(b, f.Column...)
		b = append(b, " = "...)
		if q.omitZero && table != nil {
			b = append(b, "COALESCE("...)
		}
		b = append(b, "_data."...)
		b = append(b, f.Column...)
		if q.omitZero && table != nil {
			b = append(b, ", "...)
			if table.Alias != table.SQLName {
				b = append(b, table.Alias...)
				b = append(b, '.')
			}
			b = append(b, f.Column...)
			b = append(b, ")"...)
		}
	}

	return b, nil
}

func (q *UpdateQuery) appendOtherTables(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	hasMultiTables := q.q.hasMultiTables()
	wherePKSlice := q.q.hasFlag(wherePKFlag) && q.q.tableModel.Kind() == reflect.Slice

	if !hasMultiTables && !wherePKSlice {
		return b, nil
	}

	b = append(b, " FROM "...)
	startLen := len(b)

	if hasMultiTables {
		b, err = q.q.appendOtherTables(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	if wherePKSlice {
		if len(b) > startLen {
			b = append(b, ", "...)
		}

		b, err = q.q.mustAppendSliceValues(fmter, b, false)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

func appendWhereColumnAndColumn(b []byte, alias types.Safe, fields []*Field) []byte {
	for i, f := range fields {
		if i > 0 {
			b = append(b, " AND "...)
		}
		b = append(b, alias...)
		b = append(b, '.')
		b = append(b, f.Column...)
		b = append(b, " = _data."...)
		b = append(b, f.Column...)
	}
	return b
}
