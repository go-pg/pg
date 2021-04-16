package orm

import (
	"reflect"
	"strconv"
)

type ValuesQuery struct {
	q        *Query
	ordering bool
}

var (
	_ QueryAppender = (*ValuesQuery)(nil)
	_ QueryCommand  = (*ValuesQuery)(nil)
)

func NewValuesQuery(q *Query) *ValuesQuery {
	return &ValuesQuery{
		q: q,
	}
}

func (q *ValuesQuery) String() string {
	b, err := q.AppendQuery(defaultFmter, nil)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func (q *ValuesQuery) Operation() QueryOp {
	return ValuesOp
}

func (q *ValuesQuery) Clone() QueryCommand {
	return &ValuesQuery{
		q: q.q.Clone(),
	}
}

func (q *ValuesQuery) Query() *Query {
	return q.q
}

func (q *ValuesQuery) AppendTemplate(b []byte) ([]byte, error) {
	return q.AppendQuery(dummyFormatter{}, b)
}

func (q *ValuesQuery) AppendColumns(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	fields, err := q.q.getFields()
	if err != nil {
		return nil, err
	}
	return appendColumns(b, "", fields), nil
}

func (q *ValuesQuery) AppendQuery(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	fields, err := q.q.getFields()
	if err != nil {
		return nil, err
	}
	return q.appendQuery(fmter, b, fields)
}

func (q *ValuesQuery) appendQuery(
	fmter QueryFormatter,
	b []byte,
	fields []*Field,
) (_ []byte, err error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}

	b = append(b, "VALUES ("...)

	slice := q.q.tableModel.Value()
	sliceLen := slice.Len()
	for i := 0; i < sliceLen; i++ {
		if i > 0 {
			b = append(b, "), ("...)
		}

		b, err = q.appendValues(fmter, b, fields, slice.Index(i))
		if err != nil {
			return nil, err
		}

		if q.ordering {
			b = append(b, ", "...)
			b = strconv.AppendInt(b, int64(i), 10)
		}
	}

	b = append(b, ")"...)

	return b, nil
}

func (q *ValuesQuery) appendValues(
	fmter QueryFormatter, b []byte, fields []*Field, strct reflect.Value,
) (_ []byte, err error) {
	isTemplate := isTemplateFormatter(fmter)
	for i, f := range fields {
		if i > 0 {
			b = append(b, ", "...)
		}

		app, ok := q.q.modelValues[f.SQLName]
		if ok {
			b, err = app.AppendQuery(fmter, b)
			if err != nil {
				return nil, err
			}
			continue
		}

		if isTemplate {
			b = append(b, '?')
		} else {
			b = f.AppendValue(b, indirect(strct), 1)
		}

		b = append(b, "::"...)
		b = append(b, f.SQLType...)
	}
	return b, nil
}
