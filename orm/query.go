package orm

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"gopkg.in/pg.v5/internal"
	"gopkg.in/pg.v5/types"
)

type withQuery struct {
	name  string
	query *Query
}

type Query struct {
	db        DB
	model     tableModel
	stickyErr error

	parent *Query

	tableAlias string
	with       []withQuery
	tables     []FormatAppender
	columns    []FormatAppender
	set        []queryParamsAppender
	where      []FormatAppender
	joins      []FormatAppender
	group      []queryParamsAppender
	having     []queryParamsAppender
	order      []FormatAppender
	onConflict FormatAppender
	returning  []queryParamsAppender
	limit      int
	offset     int
}

func NewQuery(db DB, model ...interface{}) *Query {
	return (&Query{}).DB(db).Model(model...)
}

// New returns new zero Query binded to the current db.
func (q *Query) New() *Query {
	return &Query{
		db: q.db,
	}
}

// Copy returns copy of the Query.
func (q *Query) Copy() *Query {
	return &Query{
		db:        q.db,
		model:     q.model,
		stickyErr: q.stickyErr,

		parent: q.parent,

		tableAlias: q.tableAlias,
		with:       q.with[:],
		tables:     q.tables[:],
		columns:    q.columns[:],
		set:        q.set[:],
		where:      q.where[:],
		joins:      q.joins[:],
		group:      q.group[:],
		having:     q.having[:],
		order:      q.order[:],
		onConflict: q.onConflict,
		returning:  q.returning[:],
		limit:      q.limit,
		offset:     q.offset,
	}
}

func (q *Query) topLevelQuery() *Query {
	if q.parent != nil {
		q.parent.with = q.with
		q.parent.With(q.parent.tableAlias, q)
		q.with = nil
		return q.parent.topLevelQuery()
	}
	return q
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

func (q *Query) WrapWith(name string) *Query {
	q.parent = q.New().Table(name).Alias(name)
	return q.parent
}

func (q *Query) Table(tables ...string) *Query {
	for _, table := range tables {
		q.tables = append(q.tables, fieldAppender{table})
	}
	return q
}

func (q *Query) TableExpr(expr string, params ...interface{}) *Query {
	q.tables = append(q.tables, queryParamsAppender{expr, params})
	return q
}

func (q *Query) Alias(alias string) *Query {
	q.tableAlias = alias
	return q
}

// Column adds column to the Query quoting it according to PostgreSQL rules.
// ColumnExpr can be used to bypass quoting restriction.
func (q *Query) Column(columns ...string) *Query {
	for _, column := range columns {
		if q.model != nil {
			if _, j := q.model.Join(column, nil); j != nil {
				continue
			}
		}

		q.columns = append(q.columns, fieldAppender{column})
	}
	return q
}

// ColumnExpr adds column expression to the Query.
func (q *Query) ColumnExpr(expr string, params ...interface{}) *Query {
	q.columns = append(q.columns, queryParamsAppender{expr, params})
	return q
}

func (q *Query) getFields() []string {
	var fields []string
	for _, col := range q.columns {
		if f, ok := col.(fieldAppender); ok {
			fields = append(fields, f.field)
		}
	}
	return fields
}

func (q *Query) Relation(name string, apply func(*Query) (*Query, error)) *Query {
	if _, j := q.model.Join(name, apply); j == nil {
		return q.err(fmt.Errorf(
			"model %s does not have relation %s",
			q.model.Table().TypeName, name,
		))
	}
	return q
}

func (q *Query) Set(set string, params ...interface{}) *Query {
	q.set = append(q.set, queryParamsAppender{set, params})
	return q
}

func (q *Query) Where(where string, params ...interface{}) *Query {
	q.where = append(q.where, queryParamsAppender{where, params})
	return q
}

func (q *Query) Join(join string, params ...interface{}) *Query {
	q.joins = append(q.joins, queryParamsAppender{join, params})
	return q
}

func (q *Query) Group(group string, params ...interface{}) *Query {
	q.group = append(q.group, queryParamsAppender{group, params})
	return q
}

func (q *Query) Having(having string, params ...interface{}) *Query {
	q.having = append(q.having, queryParamsAppender{having, params})
	return q
}

// Order adds sort order to the Query quoting column name.
// OrderExpr can be used to bypass quoting restriction.
func (q *Query) Order(orders ...string) *Query {
loop:
	for _, order := range orders {
		ind := strings.LastIndex(order, " ")
		if ind != -1 {
			field := order[:ind]
			sort := order[ind+1:]
			switch internal.ToUpper(sort) {
			case "ASC", "DESC":
				q.order = append(q.order, queryParamsAppender{
					query:  "? ?",
					params: []interface{}{types.F(field), types.Q(sort)},
				})
				continue loop
			}
		}

		q.order = append(q.order, fieldAppender{order})
		continue
	}
	return q
}

// Order adds sort order to the Query.
func (q *Query) OrderExpr(order string, params ...interface{}) *Query {
	q.order = append(q.order, queryParamsAppender{order, params})
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
	q.onConflict = queryParamsAppender{s, params}
	return q
}

func (q *Query) Returning(s string, params ...interface{}) *Query {
	q.returning = append(q.returning, queryParamsAppender{s, params})
	return q
}

// Apply calls the fn passing the Query as an argument.
func (q *Query) Apply(fn func(*Query) (*Query, error)) *Query {
	qq, err := fn(q)
	if err != nil {
		q.err(err)
		return q
	}
	return qq
}

// Count returns number of rows matching the query using count aggregate function.
func (q *Query) Count() (int, error) {
	if q.stickyErr != nil {
		return 0, q.stickyErr
	}

	q = q.Copy()
	q.columns = []FormatAppender{Q("count(*)")}
	q.order = nil
	q.limit = 0
	q.offset = 0

	var count int
	_, err := q.db.QueryOne(Scan(&count), selectQuery{q}, q.model)
	return count, err
}

// First selects the first row.
func (q *Query) First() error {
	b := columns(q.model.Table().Alias, "", q.model.Table().PKs)
	return q.OrderExpr(string(b)).Limit(1).Select()
}

// Last selects the last row.
func (q *Query) Last() error {
	b := columns(q.model.Table().Alias, "", q.model.Table().PKs)
	b = append(b, " DESC"...)
	return q.OrderExpr(string(b)).Limit(1).Select()
}

func (q *Query) newModel(values []interface{}) (model Model, err error) {
	if len(values) > 0 {
		return NewModel(values...)
	}
	return q.model, nil
}

func (q *Query) query(model Model, query interface{}) (*types.Result, error) {
	if _, ok := model.(useQueryOne); ok {
		return q.db.QueryOne(model, query, q.model)
	}
	return q.db.Query(model, query, q.model)
}

// Select selects the model.
func (q *Query) Select(values ...interface{}) error {
	if q.stickyErr != nil {
		return q.stickyErr
	}

	model, err := q.newModel(values)
	if err != nil {
		return err
	}

	res, err := q.query(model, q.selectQuery())
	if err != nil {
		return err
	}

	if res.RowsReturned() > 0 {
		if q.model != nil {
			if err := selectJoins(q.db, q.model.GetJoins()); err != nil {
				return err
			}
		}
		if err := model.AfterSelect(q.db); err != nil {
			return err
		}
	}

	return nil
}

func (q *Query) selectQuery() selectQuery {
	return selectQuery{q.topLevelQuery()}
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

func (q *Query) forEachHasOneJoin(fn func(*join)) {
	if q.model == nil {
		return
	}
	q._forEachHasOneJoin(q.model.GetJoins(), fn)
}

func (q *Query) _forEachHasOneJoin(joins []join, fn func(*join)) {
	for i := range joins {
		j := &joins[i]
		switch j.Rel.Type {
		case HasOneRelation, BelongsToRelation:
			fn(j)
			q._forEachHasOneJoin(j.JoinModel.GetJoins(), fn)
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

	model, err := q.newModel(values)
	if err != nil {
		return nil, err
	}

	if q.model != nil {
		if err := q.model.BeforeInsert(q.db); err != nil {
			return nil, err
		}
	}

	res, err := q.db.Query(model, insertQuery{Query: q}, q.model)
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
		if res.RowsAffected() == 1 {
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

	if q.model != nil {
		if err := q.model.BeforeUpdate(q.db); err != nil {
			return nil, err
		}
	}

	res, err := q.db.Query(model, updateQuery{q}, q.model)
	if err != nil {
		return nil, err
	}

	if q.model != nil {
		if err := q.model.AfterUpdate(q.db); err != nil {
			return nil, err
		}
	}

	return res, nil
}

// Delete deletes the model.
func (q *Query) Delete() (*types.Result, error) {
	if q.stickyErr != nil {
		return nil, q.stickyErr
	}

	if q.model != nil {
		if err := q.model.BeforeDelete(q.db); err != nil {
			return nil, err
		}
	}

	res, err := q.db.Query(q.model, deleteQuery{q}, q.model)
	if err != nil {
		return nil, err
	}

	if q.model != nil {
		if err := q.model.AfterDelete(q.db); err != nil {
			return nil, err
		}
	}

	return res, nil
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

func (q *Query) hasTables() bool {
	return q.model != nil || len(q.tables) > 0
}

func (q *Query) appendTables(b []byte) []byte {
	if q.model != nil {
		b = q.appendTableNameWithAlias(b)
		if len(q.tables) > 0 {
			b = append(b, ", "...)
		}
	}
	for i, f := range q.tables {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = f.AppendFormat(b, q)
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
	return wherePKQuery{q}.AppendFormat(b, nil), nil
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

type wherePKQuery struct {
	*Query
}

func (q wherePKQuery) AppendFormat(b []byte, f QueryFormatter) []byte {
	table := q.model.Table()
	return appendColumnAndValue(b, q.model.Value(), table, table.PKs)
}
