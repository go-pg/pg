package orm

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"gopkg.in/pg.v4/internal"
	"gopkg.in/pg.v4/types"
)

type withQuery struct {
	name  string
	query *Query
}

type Query struct {
	db        DB
	model     tableModel
	stickyErr error

	tableAlias string
	with       []withQuery
	tables     []string
	fields     []string
	columns    []FormatAppender
	rels       map[string]func(*Query) (*Query, error)
	set        []queryParams
	where      []FormatAppender
	joins      []FormatAppender
	group      []queryParams
	order      []queryParams
	onConflict FormatAppender
	returning  []queryParams
	limit      int
	offset     int
}

func NewQuery(db DB, model ...interface{}) *Query {
	return (&Query{}).DB(db).Model(model...)
}

func (q *Query) copy() *Query {
	cp := *q
	return &cp
}

func (q *Query) err(err error) *Query {
	if q.stickyErr == nil {
		q.stickyErr = err
	}
	return q
}

func (q *Query) DB(db DB) *Query {
	q.db = db
	return q
}

func (q *Query) Model(model ...interface{}) *Query {
	var err error
	switch l := len(model); {
	case l == 0:
		q.model = nil
	case l == 1:
		model0 := model[0]
		if model0 != nil {
			q.model, err = newTableModel(model0)
		}
	case l > 1:
		q.model, err = newTableModel(&model)
	}
	if err != nil {
		q = q.err(err)
	}
	return q
}

func (q *Query) With(name string, subq *Query) *Query {
	q.with = append(q.with, withQuery{name, subq})
	return q
}

func (q *Query) Table(tables ...string) *Query {
	for _, table := range tables {
		q.tables = append(q.tables, table)
	}
	return q
}

func (q *Query) Alias(alias string) *Query {
	q.tableAlias = alias
	return q
}

func (q *Query) Column(columns ...string) *Query {
loop:
	for _, column := range columns {
		if q.model != nil {
			if j := q.model.Join(column, nil); j != nil {
				continue loop
			}
		}

		q.fields = append(q.fields, column)
		q.columns = append(q.columns, fieldParams{field: column})
	}
	return q
}

func (q *Query) ColumnExpr(expr string, params ...interface{}) *Query {
	q.columns = append(q.columns, queryParams{expr, params})
	return q
}

func (q *Query) Relation(name string, apply func(*Query) (*Query, error)) *Query {
	if j := q.model.Join(name, apply); j == nil {
		return q.err(fmt.Errorf(
			"model %s does not have relation %s",
			q.model.Table().Type.Name(), name,
		))
	}
	return q
}

func (q *Query) Set(set string, params ...interface{}) *Query {
	q.set = append(q.set, queryParams{set, params})
	return q
}

func (q *Query) Where(where string, params ...interface{}) *Query {
	q.where = append(q.where, queryParams{where, params})
	return q
}

func (q *Query) Join(join string, params ...interface{}) *Query {
	q.joins = append(q.joins, queryParams{join, params})
	return q
}

func (q *Query) Group(group string, params ...interface{}) *Query {
	q.group = append(q.group, queryParams{group, params})
	return q
}

func (q *Query) Order(order string, params ...interface{}) *Query {
	q.order = append(q.order, queryParams{order, params})
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
	q.onConflict = queryParams{s, params}
	return q
}

func (q *Query) Returning(s string, params ...interface{}) *Query {
	q.returning = append(q.returning, queryParams{s, params})
	return q
}

// Apply calls the fn passing the Query as an argument.
func (q *Query) Apply(fn func(*Query) *Query) *Query {
	return fn(q)
}

// Count returns number of rows matching the query using count aggregate function.
func (q *Query) Count() (int, error) {
	if q.stickyErr != nil {
		return 0, q.stickyErr
	}

	q = q.copy()
	q.columns = append(q.columns, Q("count(*)"))
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

func (q *Query) newModel(values []interface{}) (model Model, err error) {
	if len(values) > 0 {
		return NewModel(values...)
	}
	return q.model, nil
}

// Select selects the model.
func (q *Query) Select(values ...interface{}) error {
	if q.stickyErr != nil {
		return q.stickyErr
	}

	if q.model != nil {
		q.addJoins(q.model.GetJoins())
	}
	sel := selectQuery{q}

	model, err := q.newModel(values)
	if err != nil {
		return err
	}

	var res *types.Result
	if m, ok := model.(useQueryOne); ok && m.useQueryOne() {
		res, err = q.db.QueryOne(model, sel, q.model)
	} else {
		res, err = q.db.Query(model, sel, q.model)
	}
	if err != nil {
		return err
	}

	if q.model != nil && res.Affected() > 0 {
		if err := selectJoins(q.db, q.model.GetJoins()); err != nil {
			return err
		}
		if err := model.AfterSelect(q.db); err != nil {
			return err
		}
	}

	return nil
}

// SelectAndCount runs Select and Count in two separate goroutines,
// waits for them to finish and returns the result.
func (q *Query) SelectAndCount(values ...interface{}) (count int, err error) {
	if q.stickyErr != nil {
		return 0, q.stickyErr
	}

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

func (q *Query) addJoins(joins []join) {
	for i := range joins {
		j := &joins[i]
		switch j.Rel.Type {
		case HasOneRelation:
			j.JoinHasOne(q)
			q.addJoins(j.JoinModel.GetJoins())
		case BelongsToRelation:
			j.JoinBelongsTo(q)
			q.addJoins(j.JoinModel.GetJoins())
		}
	}
}

func selectJoins(db DB, joins []join) error {
	var err error
	for i := range joins {
		j := &joins[i]
		if j.Rel.Type == HasOneRelation || j.Rel.Type == BelongsToRelation {
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

// Insert inserts the model.
func (q *Query) Insert(values ...interface{}) (*types.Result, error) {
	if q.stickyErr != nil {
		return nil, q.stickyErr
	}

	var model Model
	if len(values) > 0 {
		model = Scan(values...)
	} else if q.model != nil {
		model = q.model
	}

	if q.model != nil {
		if err := q.model.BeforeInsert(q.db); err != nil {
			return nil, err
		}
	}

	ins := insertQuery{Query: q}
	res, err := q.db.Query(model, ins, q.model)
	if err != nil {
		return nil, err
	}

	if q.model != nil {
		if err := q.model.AfterInsert(q.db); err != nil {
			return nil, err
		}
	}

	return res, nil
}

// SelectOrInsert selects the model inserting one if it does not exist.
func (q *Query) SelectOrInsert(values ...interface{}) (inserted bool, err error) {
	if q.stickyErr != nil {
		return false, q.stickyErr
	}

	var insertErr error
	for i := 0; i < 5; i++ {
		if i >= 2 {
			time.Sleep(internal.RetryBackoff << uint(i-2))
		}

		err := q.Select(values...)
		if err == nil {
			return false, nil
		}
		if err != internal.ErrNoRows {
			return false, err
		}

		res, err := q.Insert(values...)
		if err != nil {
			insertErr = err
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

	err = fmt.Errorf(
		"pg: SelectOrInsert: select returns no rows (insert fails with err=%q)",
		insertErr,
	)
	return false, err
}

// Update updates the model.
func (q *Query) Update(values ...interface{}) (*types.Result, error) {
	if q.stickyErr != nil {
		return nil, q.stickyErr
	}

	model, err := q.newModel(values)
	if err != nil {
		return nil, err
	}

	return q.db.Query(model, updateQuery{q}, q.model)
}

// Delete deletes the model.
func (q *Query) Delete() (*types.Result, error) {
	if q.stickyErr != nil {
		return nil, q.stickyErr
	}
	return q.db.Exec(deleteQuery{q}, q.model)
}

func (q *Query) FormatQuery(dst []byte, query string, params ...interface{}) []byte {
	params = append(params, q.model)
	if q.db != nil {
		return q.db.FormatQuery(dst, query, params...)
	}
	return Formatter{}.Append(dst, query, params...)
}

func (q *Query) appendTableAlias(b []byte) ([]byte, bool) {
	if q.tableAlias != "" {
		return types.AppendField(b, q.tableAlias, 1), true
	}
	if q.model != nil {
		return append(b, q.model.Table().Alias...), true
	}
	return b, false
}

func (q *Query) appendTableName(b []byte) []byte {
	return q.FormatQuery(b, string(q.model.Table().Name))
}

func (q *Query) appendTableNameWithAlias(b []byte) []byte {
	b = q.appendTableName(b)
	b = append(b, " AS "...)
	b, _ = q.appendTableAlias(b)
	return b
}

func (q *Query) haveTables() bool {
	return q.model != nil || len(q.tables) > 0
}

func (q *Query) appendTables(b []byte) []byte {
	if q.model != nil {
		b = q.appendTableNameWithAlias(b)
		if len(q.tables) > 0 {
			b = append(b, ", "...)
		}
	}
	for i, table := range q.tables {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = types.AppendField(b, table, 1)
	}
	return b
}

func (q *Query) mustAppendWhere(b []byte) ([]byte, error) {
	if len(q.where) > 0 {
		b = q.appendWhere(b)
		return b, nil
	}

	if q.model == nil {
		return nil, errors.New("pg: Model(nil)")
	}

	if err := q.model.Table().checkPKs(); err != nil {
		return nil, err
	}

	b = append(b, " WHERE "...)
	return pkWhereQuery{q}.AppendFormat(b, nil), nil
}

func (q *Query) appendWhere(b []byte) []byte {
	b = append(b, " WHERE "...)
	for i, f := range q.where {
		if i > 0 {
			b = append(b, " AND "...)
		}
		b = append(b, '(')
		b = f.AppendFormat(b, q)
		b = append(b, ')')
	}
	return b
}

func (q *Query) appendSet(b []byte) []byte {
	b = append(b, " SET "...)
	for i, f := range q.set {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = f.AppendFormat(b, q)
	}
	return b
}

func (q *Query) appendReturning(b []byte) []byte {
	b = append(b, " RETURNING "...)
	for i, f := range q.returning {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = f.AppendFormat(b, q)
	}
	return b
}

type pkWhereQuery struct {
	*Query
}

func (q pkWhereQuery) AppendFormat(b []byte, f QueryFormatter) []byte {
	table := q.model.Table()
	return appendColumnAndValue(b, q.model.Value(), table, table.PKs)
}
