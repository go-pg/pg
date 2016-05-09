package orm

import (
	"errors"
	"fmt"
	"sync"

	"gopkg.in/pg.v4/internal"
	"gopkg.in/pg.v4/types"
)

type Query struct {
	db    dber
	model tableModel
	err   error

	tableName  types.Q
	tableAlias string

	tables     []byte
	fields     []string
	columns    []byte
	set        []byte
	where      []byte
	join       []byte
	group      []byte
	order      []byte
	onConflict []byte
	returning  []byte
	limit      int
	offset     int
}

func NewQuery(db dber, v interface{}) *Query {
	model, err := newTableModel(v)
	q := Query{
		db:    db,
		model: model,
		err:   err,
	}
	if err == nil {
		q.tableName = q.format(nil, string(q.model.Table().Name))
	}
	return &q
}

func (q *Query) copy() *Query {
	cp := *q
	return &cp
}

func (q *Query) setErr(err error) {
	if q.err == nil {
		q.err = err
	}
}

func (q *Query) Alias(alias string) *Query {
	q.tableAlias = alias
	return q
}

func (q *Query) Table(names ...string) *Query {
	for _, name := range names {
		q.tables = types.AppendField(q.tables, name, 1)
	}
	return q
}

func (q *Query) Column(columns ...string) *Query {
loop:
	for _, column := range columns {
		if j := q.model.Join(column); j != nil {
			continue loop
		}

		q.fields = append(q.fields, column)
		q.columns = appendSep(q.columns, ", ")
		q.columns = types.AppendField(q.columns, column, 1)
	}
	return q
}

func (q *Query) ColumnExpr(expr string, params ...interface{}) *Query {
	q.columns = appendSep(q.columns, ", ")
	q.columns = q.format(q.columns, expr, params...)
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

func (q *Query) Group(group string, params ...interface{}) *Query {
	q.group = appendSep(q.group, ", ")
	q.group = q.format(q.group, group, params...)
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

// Count returns number of rows matching the query using count aggregate function.
func (q *Query) Count() (int, error) {
	if q.err != nil {
		return 0, q.err
	}

	q = q.copy()
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

// First selects the first row.
func (q *Query) First() error {
	b := columns(q.model.Table().Alias, "", q.model.Table().PKs)
	return q.Order(string(b)).Limit(1).Select()
}

// Last selects the last row.
func (q *Query) Last() error {
	b := columns(q.model.Table().Alias, "", q.model.Table().PKs)
	b = append(b, " DESC"...)
	return q.Order(string(b)).Limit(1).Select()
}

// Select selects the model.
func (q *Query) Select(values ...interface{}) error {
	q.joinHasOne()
	sel := selectQuery{q}

	var model Model
	var err error
	if len(values) > 0 {
		model, err = NewModel(values...)
		if err != nil {
			return err
		}
	} else {
		model = q.model
	}

	if m, ok := model.(useQueryOne); ok && m.useQueryOne() {
		_, err = q.db.QueryOne(model, sel, q.model)
	} else {
		_, err = q.db.Query(model, sel, q.model)
	}
	if err != nil {
		return err
	}

	return selectJoins(q.db, q.model.GetJoins())
}

// SelectAndCount runs Select and Count in two separate goroutines,
// waits for them to finish and returns the result.
func (q *Query) SelectAndCount(values ...interface{}) (count int, err error) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		if e := q.Select(values...); e != nil {
			err = e
		}
	}()

	go func() {
		defer wg.Done()
		var e error
		count, e = q.Count()
		if e != nil {
			err = e
		}
	}()

	wg.Wait()
	return count, err
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

func selectJoins(db dber, joins []join) error {
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

// Create inserts the model.
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

// SelectOrCreate selects the model creating one if it does not exist.
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

// Update updates the model.
func (q *Query) Update() (*types.Result, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.db.Query(q.model, updateQuery{q}, q.model)
}

// Delete deletes the model.
func (q *Query) Delete() (*types.Result, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.db.Exec(deleteQuery{q}, q.model)
}

func (q *Query) format(dst []byte, query string, params ...interface{}) []byte {
	params = append(params, q.model)
	return q.db.FormatQuery(dst, query, params...)
}

func (q *Query) appendTableNameWithAlias(b []byte) []byte {
	b = append(b, q.tableName...)
	b = append(b, " AS "...)
	if q.tableAlias != "" {
		b = types.AppendField(b, q.tableAlias, 1)
	} else {
		b = append(b, q.model.Table().Alias...)
	}
	return b
}

func (q *Query) appendSet(b []byte) ([]byte, error) {
	b = append(b, " SET "...)
	if len(q.set) > 0 {
		b = append(b, q.set...)
	} else if len(q.fields) > 0 {
		table := q.model.Table()
		strct := q.model.Value()
		for i, fieldName := range q.fields {
			field, err := table.GetField(fieldName)
			if err != nil {
				return nil, err
			}

			b = append(b, field.ColName...)
			b = append(b, " = "...)
			b = field.AppendValue(b, strct, 1)
			if i != len(q.fields)-1 {
				b = append(b, ", "...)
			}
		}
	} else {
		table := q.model.Table()
		strct := q.model.Value()

		start := len(b)
		for _, field := range table.Fields {
			if field.Has(PrimaryKeyFlag) {
				continue
			}

			b = append(b, field.ColName...)
			b = append(b, " = "...)
			b = field.AppendValue(b, strct, 1)
			b = append(b, ", "...)
		}
		if len(b) > start {
			b = b[:len(b)-2]
		}
	}
	return b, nil
}

func (q *Query) appendWhere(b []byte) ([]byte, error) {
	b = append(b, " WHERE "...)
	if len(q.where) > 0 {
		b = append(b, q.where...)
	} else {
		table := q.model.Table()
		if err := table.checkPKs(); err != nil {
			return nil, err
		}
		b = appendColumnAndValue(b, q.model.Value(), table, table.PKs)
	}
	return b, nil
}
