package orm

import (
	"errors"
	"fmt"
	"reflect"

	"gopkg.in/pg.v4/internal"
	"gopkg.in/pg.v4/types"
)

type Query struct {
	db    dber
	model TableModel
	err   error

	tableName  types.Q
	tables     []byte
	fields     []string
	columns    []byte
	set        []byte
	where      []byte
	join       []byte
	order      []byte
	onConflict []byte
	returning  []byte
	limit      int
	offset     int
}

func NewQuery(db dber, v interface{}) *Query {
	model, err := NewTableModel(v)
	q := Query{
		db:    db,
		model: model,
		err:   err,
	}
	if err == nil {
		q.tableName = q.format(nil, string(q.model.Table().Name))

		q.tables = appendSep(q.tables, ", ")
		q.tables = append(q.tables, q.tableName...)
		q.tables = append(q.tables, " AS "...)
		q.tables = types.AppendField(q.tables, q.model.Table().ModelName, 1)
	}
	return &q
}

func (q *Query) setErr(err error) {
	if q.err == nil {
		q.err = err
	}
}

func (q *Query) Table(names ...string) *Query {
	for _, name := range names {
		q.tables = types.AppendField(q.tables, name, 1)
	}
	return q
}

func (q *Query) Column(columns ...interface{}) *Query {
loop:
	for _, column := range columns {
		switch column := column.(type) {
		case string:
			if j := q.model.Join(column); j != nil {
				continue loop
			}

			q.fields = append(q.fields, column)
			q.columns = appendSep(q.columns, ", ")
			q.columns = types.AppendField(q.columns, column, 1)
		case types.ValueAppender:
			var err error
			q.columns = appendSep(q.columns, ", ")
			q.columns, err = column.AppendValue(q.columns, 1)
			if err != nil {
				q.setErr(err)
			}
		default:
			q.setErr(fmt.Errorf("unsupported column type: %T", column))
		}
	}
	return q
}

func (q *Query) Set(set string, params ...interface{}) *Query {
	q.set = appendSep(q.set, ", ")
	q.set = q.format(q.set, set, params...)
	return q
}

func (q *Query) Where(where string, params ...interface{}) *Query {
	q.where = appendSep(q.where, " AND ")
	q.where = append(q.where, '(')
	q.where = q.format(q.where, where, params...)
	q.where = append(q.where, ')')
	return q
}

func (q *Query) Join(join string, params ...interface{}) *Query {
	q.join = appendSep(q.join, " ")
	q.join = q.format(q.join, join, params...)
	return q
}

func (q *Query) Order(order string, params ...interface{}) *Query {
	q.order = appendSep(q.order, ", ")
	q.order = q.format(q.order, order, params...)
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

func (q *Query) OnConflict(s string, params ...interface{}) *Query {
	q.onConflict = q.format(nil, s, params...)
	return q
}

func (q *Query) Returning(columns ...interface{}) *Query {
	for _, column := range columns {
		q.returning = appendSep(q.returning, ", ")

		switch column := column.(type) {
		case string:
			q.returning = types.AppendField(q.returning, column, 1)
		case types.ValueAppender:
			var err error
			q.returning, err = column.AppendValue(q.returning, 1)
			if err != nil {
				q.setErr(err)
			}
		default:
			q.setErr(fmt.Errorf("unsupported column type: %T", column))
		}
	}
	return q
}

func (q *Query) Count() (int, error) {
	if q.err != nil {
		return 0, q.err
	}

	q.joinHasOne()
	q.columns = types.Q("COUNT(*)")
	q.order = nil
	q.limit = 0
	q.offset = 0

	sel := &selectQuery{
		Query: q,
	}
	var count int
	_, err := q.db.QueryOne(Scan(&count), sel, q.model)
	return count, err
}

func (q *Query) First() error {
	b := columns(col(q.model.Table().ModelName), "", q.model.Table().PKs)
	return q.Order(string(b)).Limit(1).Select()
}

func (q *Query) Last() error {
	b := columns(col(q.model.Table().ModelName), "", q.model.Table().PKs)
	b = append(b, " DESC"...)
	return q.Order(string(b)).Limit(1).Select()
}

// Select selects the model from database.
func (q *Query) Select(values ...interface{}) error {
	q.joinHasOne()
	sel := selectQuery{q}

	var err error
	if len(values) > 0 {
		_, err = q.db.QueryOne(Scan(values...), sel, q.model)
	} else if q.model.Kind() == reflect.Slice {
		_, err = q.db.Query(q.model, sel, q.model)
	} else {
		_, err = q.db.QueryOne(q.model, sel, q.model)
	}
	if err != nil {
		return err
	}

	return selectJoins(q.db, q.model.GetJoins())
}

func (q *Query) joinHasOne() {
	joins := q.model.GetJoins()
	for i := range joins {
		j := &joins[i]
		if j.Rel.One {
			j.JoinOne(q)
		}
	}
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

// Create inserts the model into database.
func (q *Query) Create(values ...interface{}) (*types.Result, error) {
	if q.err != nil {
		return nil, q.err
	}

	var model Model
	if len(values) > 0 {
		model = Scan(values...)
	} else {
		model = q.model
	}

	return q.db.Query(model, insertQuery{Query: q}, q.model)
}

// SelectOrCreate selects the model from database creating one if necessary.
func (q *Query) SelectOrCreate(values ...interface{}) (created bool, err error) {
	if q.err != nil {
		return false, q.err
	}

	for i := 0; i < 10; i++ {
		err := q.Select(values...)
		if err == nil {
			return false, nil
		}
		if err != internal.ErrNoRows {
			return false, err
		}

		res, err := q.Create(values...)
		if err != nil {
			if pgErr, ok := err.(internal.PGError); ok {
				if pgErr.IntegrityViolation() {
					continue
				}
				if pgErr.Field('C') == "55000" {
					// Retry on "#55000 attempted to delete invisible tuple".
					continue
				}
			}
			return false, err
		}
		if res.Affected() == 1 {
			return true, nil
		}
	}

	return false, errors.New("pg: GetOrCreate does not make progress after 10 iterations")
}

// Update updates the model in database.
func (q *Query) Update() (*types.Result, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.db.Query(q.model, updateModel{q}, q.model)
}

// Update updates the model using provided values.
func (q *Query) UpdateValues(values map[string]interface{}) (*types.Result, error) {
	if q.err != nil {
		return nil, q.err
	}
	upd := updateQuery{
		Query: q,
		data:  values,
	}
	return q.db.Query(q.model, upd, q.model)
}

// Delete deletes the model from database.
func (q *Query) Delete() (*types.Result, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.db.Exec(deleteModel{q}, q.model)
}

func (q *Query) format(dst []byte, query string, params ...interface{}) []byte {
	params = append(params, q.model)
	return q.db.FormatQuery(dst, query, params...)
}
