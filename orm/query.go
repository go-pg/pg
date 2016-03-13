package orm

import (
	"fmt"
	"reflect"

	"gopkg.in/pg.v4/types"
)

type Query struct {
	db    dber
	model TableModel
	err   error

	tables    []string
	columns   []types.ValueAppender
	returning []types.ValueAppender
	wheres    [][]byte
	joins     [][]byte
	orders    []string
	limit     int
	offset    int
}

func NewQuery(db dber, v interface{}) *Query {
	model, err := NewTableModel(v)
	q := Query{
		db:    db,
		model: model,
		err:   err,
	}
	if err == nil {
		q.tables = append(q.tables, q.model.Table().Name+" AS "+q.model.Table().ModelName)
	}
	return &q
}

func (q *Query) setErr(err error) {
	if q.err == nil {
		q.err = err
	}
}

func (q *Query) Columns(columns ...interface{}) *Query {
loop:
	for _, column := range columns {
		switch column := column.(type) {
		case string:
			if _, err := q.model.Join(column); err == nil {
				continue loop
			}
			q.columns = append(q.columns, types.F(column))
		case types.ValueAppender:
			q.columns = append(q.columns, column)
		default:
			panic(fmt.Sprintf("unsupported column type: %T", column))
		}
	}
	return q
}

func (q *Query) Returning(columns ...interface{}) *Query {
	for _, column := range columns {
		switch column := column.(type) {
		case string:
			q.returning = append(q.returning, types.F(column))
		case types.ValueAppender:
			q.returning = append(q.returning, column)
		default:
			panic(fmt.Sprintf("unsupported column type: %T", column))
		}
	}
	return q
}

func (q *Query) Table(name string) *Query {
	q.tables = append(q.tables, name)
	return q
}

func (q *Query) Where(where string, params ...interface{}) *Query {
	if false {
		for i, param := range params {
			if f, ok := param.(types.F); ok {
				column, err := q.model.Join(string(f) + "._")
				if err == nil {
					params[i] = types.F(column)
				}
			}
		}
	}

	b, err := Format(nil, where, params...)
	if err != nil {
		q.setErr(err)
	} else {
		q.wheres = append(q.wheres, b)
	}

	return q
}

func (q *Query) Join(join string, params ...interface{}) *Query {
	b, err := Format(nil, join, params...)
	if err != nil {
		q.setErr(err)
	} else {
		q.joins = append(q.joins, b)
	}
	return q
}

func (q *Query) Order(order string) *Query {
	q.orders = append(q.orders, order)
	return q
}

func (q *Query) Limit(n int) *Query {
	q.limit = n
	return q
}

func (q *Query) Offset(n int) *Query {
	q.offset = n
	return q
}

func (q *Query) Count(count *int) error {
	if q.err != nil {
		return q.err
	}

	q.columns = []types.ValueAppender{types.Q("COUNT(*)")}
	q.orders = nil
	q.limit = 0
	q.offset = 0

	joins := q.model.GetJoins()
	for i := range joins {
		j := &joins[i]
		if j.Rel.One {
			j.JoinOne(q)
		}
	}

	sel := &selectQuery{
		Query: q,
	}

	_, err := q.db.Query(Scan(count), sel, q.model)
	if err != nil {
		return err
	}

	return nil
}

func (q *Query) First() error {
	b := appendPKs(nil, q.model.Table().PKs)
	return q.Order(string(b)).Limit(1).Select()
}

func (q *Query) Last() error {
	b := appendPKs(nil, q.model.Table().PKs)
	b = append(b, " DESC"...)
	return q.Order(string(b)).Limit(1).Select()
}

func (q *Query) Select() error {
	return q.selectModel(q.model)
}

func (q *Query) selectModel(model TableModel) error {
	joins := model.GetJoins()
	for i := range joins {
		j := &joins[i]
		if j.Rel.One {
			j.JoinOne(q)
		}
	}

	sel := &selectQuery{
		Query: q,
	}
	var err error
	if model.Kind() == reflect.Slice {
		_, err = q.db.Query(model, sel, model)
	} else {
		_, err = q.db.QueryOne(model, sel, model)
	}
	if err != nil {
		return err
	}

	return selectJoins(q.db, joins)
}

func selectJoins(db dber, joins []Join) error {
	var err error
	for i := range joins {
		j := &joins[i]
		if j.Rel.One {
			err = selectJoins(db, j.JoinModel.GetJoins())
		} else {
			err = j.Select(db)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *Query) Update() error {
	if q.err != nil {
		return q.err
	}
	upd := &updateModel{
		Query: q,
	}
	_, err := q.db.Query(q.model, upd, upd.model)
	return err
}

func (q *Query) UpdateValues(data map[string]interface{}) error {
	if q.err != nil {
		return q.err
	}
	upd := &updateQuery{
		Query: q,
		data:  data,
	}
	_, err := q.db.Query(upd.model, upd, upd.model)
	return err
}

func (q *Query) Delete() error {
	if q.err != nil {
		return q.err
	}
	del := deleteQuery{
		Query: q,
	}
	_, err := q.db.Exec(del, del.model)
	return err
}
