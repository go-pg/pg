package orm

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/types"
)

type withQuery struct {
	name  string
	query *Query
}

type Query struct {
	db        DB
	stickyErr error

	model       tableModel
	ignoreModel bool

	with       []withQuery
	tables     []FormatAppender
	columns    []FormatAppender
	set        []FormatAppender
	where      []sepFormatAppender
	joins      []FormatAppender
	group      []FormatAppender
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

// New returns new zero Query binded to the current db and model.
func (q *Query) New() *Query {
	return &Query{
		db:          q.db,
		model:       q.model,
		ignoreModel: true,
	}
}

// Copy returns copy of the Query.
func (q *Query) Copy() *Query {
	copy := &Query{
		db:        q.db,
		stickyErr: q.stickyErr,

		model:       q.model,
		ignoreModel: q.ignoreModel,

		tables:     q.tables[:len(q.tables):len(q.tables)],
		columns:    q.columns[:len(q.columns):len(q.columns)],
		set:        q.set[:len(q.set):len(q.set)],
		where:      q.where[:len(q.where):len(q.where)],
		joins:      q.joins[:len(q.joins):len(q.joins)],
		group:      q.group[:len(q.group):len(q.group)],
		having:     q.having[:len(q.having):len(q.having)],
		order:      q.order[:len(q.order):len(q.order)],
		onConflict: q.onConflict,
		returning:  q.returning[:len(q.returning):len(q.returning)],
		limit:      q.limit,
		offset:     q.offset,
	}
	for _, with := range q.with {
		copy = copy.With(with.name, with.query.Copy())
	}
	return copy
}

func (q *Query) err(err error) *Query {
	if q.stickyErr == nil {
		q.stickyErr = err
	}
	return q
}

func (q *Query) DB(db DB) *Query {
	q.db = db
	for _, with := range q.with {
		with.query.db = db
	}
	return q
}

func (q *Query) Model(model ...interface{}) *Query {
	var err error
	switch l := len(model); {
	case l == 0:
		q.model = nil
	case l == 1:
		q.model, err = newTableModel(model[0])
	case l > 1:
		q.model, err = newTableModel(&model)
	}
	if err != nil {
		q = q.err(err)
	}
	if q.ignoreModel {
		q.ignoreModel = false
	}
	return q
}

// With adds subq as common table expression with the given name.
func (q *Query) With(name string, subq *Query) *Query {
	q.with = append(q.with, withQuery{name, subq})
	return q
}

// WrapWith creates new Query and adds to it current query as
// common table expression with the given name.
func (q *Query) WrapWith(name string) *Query {
	wrapper := q.New()
	wrapper.with = q.with
	q.with = nil
	wrapper = wrapper.With(name, q)
	return wrapper
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

// Column adds column to the Query quoting it according to PostgreSQL rules.
// ColumnExpr can be used to bypass quoting restriction.
func (q *Query) Column(columns ...string) *Query {
	for _, column := range columns {
		if column == "_" {
			if q.columns == nil {
				q.columns = make([]FormatAppender, 0)
			}
			continue
		}

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
			"model=%s does not have relation=%s",
			q.model.Table().Type.Name(), name,
		))
	}
	return q
}

func (q *Query) Set(set string, params ...interface{}) *Query {
	q.set = append(q.set, queryParamsAppender{set, params})
	return q
}

func (q *Query) Where(where string, params ...interface{}) *Query {
	q.where = append(q.where, &whereAppender{"AND", where, params})
	return q
}

func (q *Query) WhereOr(where string, params ...interface{}) *Query {
	q.where = append(q.where, &whereAppender{"OR", where, params})
	return q
}

// WhereIn is a shortcut for Where and pg.In to work with IN operator:
//
//    WhereIn("id IN (?)", 1, 2, 3)
func (q *Query) WhereIn(where string, params ...interface{}) *Query {
	return q.Where(where, types.In(params))
}

func (q *Query) Join(join string, params ...interface{}) *Query {
	q.joins = append(q.joins, queryParamsAppender{join, params})
	return q
}

func (q *Query) Group(columns ...string) *Query {
	for _, column := range columns {
		q.group = append(q.group, fieldAppender{column})
	}
	return q
}

func (q *Query) GroupExpr(group string, params ...interface{}) *Query {
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
		ind := strings.Index(order, " ")
		if ind != -1 {
			field := order[:ind]
			sort := order[ind+1:]
			switch internal.ToUpper(sort) {
			case "ASC", "DESC", "ASC NULLS FIRST", "DESC NULLS FIRST",
				"ASC NULLS LAST", "DESC NULLS LAST":
				q = q.OrderExpr("? ?", types.F(field), types.Q(sort))
				continue loop
			}
		}

		q.order = append(q.order, fieldAppender{order})
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

	var count int
	_, err := q.db.QueryOne(
		Scan(&count),
		q.countQuery().countSelectQuery("count(*)"),
		q.model,
	)
	return count, err
}

func (q *Query) countQuery() *Query {
	if len(q.group) > 0 {
		return q.Copy().WrapWith("wrapper").Table("wrapper")
	}
	return q
}

func (q *Query) countSelectQuery(column string) selectQuery {
	return selectQuery{
		q:     q,
		count: column,
	}
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

// Select selects the model.
func (q *Query) Select(values ...interface{}) error {
	if q.stickyErr != nil {
		return q.stickyErr
	}

	model, err := q.newModel(values...)
	if err != nil {
		return err
	}

	res, err := q.query(model, selectQuery{q: q})
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

func (q *Query) newModel(values ...interface{}) (Model, error) {
	if len(values) > 0 {
		return NewModel(values...)
	}
	return q.model, nil
}

func (q *Query) query(model Model, query interface{}) (Result, error) {
	if _, ok := model.(useQueryOne); ok {
		return q.db.QueryOne(model, query, q.model)
	}
	return q.db.Query(model, query, q.model)
}

// SelectAndCount runs Select and Count in two goroutines,
// waits for them to finish and returns the result.
func (q *Query) SelectAndCount(values ...interface{}) (count int, err error) {
	if q.stickyErr != nil {
		return 0, q.stickyErr
	}

	var wg sync.WaitGroup
	wg.Add(2)
	var mu sync.Mutex

	go func() {
		defer wg.Done()
		if e := q.Select(values...); e != nil {
			mu.Lock()
			err = e
			mu.Unlock()
		}
	}()

	go func() {
		defer wg.Done()
		var e error
		count, e = q.Count()
		if e != nil {
			mu.Lock()
			err = e
			mu.Unlock()
		}
	}()

	wg.Wait()
	return count, err
}

func (q *Query) forEachHasOneJoin(fn func(*join)) {
	if q.model == nil {
		return
	}
	q._forEachHasOneJoin(fn, q.model.GetJoins())
}

func (q *Query) _forEachHasOneJoin(fn func(*join), joins []join) {
	for i := range joins {
		j := &joins[i]
		switch j.Rel.Type {
		case HasOneRelation, BelongsToRelation:
			fn(j)
			q._forEachHasOneJoin(fn, j.JoinModel.GetJoins())
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
func (q *Query) Insert(values ...interface{}) (Result, error) {
	if q.stickyErr != nil {
		return nil, q.stickyErr
	}

	model, err := q.newModel(values...)
	if err != nil {
		return nil, err
	}

	if q.model != nil {
		if err := q.model.BeforeInsert(q.db); err != nil {
			return nil, err
		}
	}

	res, err := q.db.Query(model, insertQuery{q: q}, q.model)
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
			time.Sleep(internal.RetryBackoff(i - 2))
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
func (q *Query) Update(values ...interface{}) (Result, error) {
	if q.stickyErr != nil {
		return nil, q.stickyErr
	}

	model, err := q.newModel(values...)
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
func (q *Query) Delete() (Result, error) {
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

func (q *Query) CreateTable(opt *CreateTableOptions) (Result, error) {
	if q.stickyErr != nil {
		return nil, q.stickyErr
	}
	return q.db.Exec(createTableQuery{
		q:   q,
		opt: opt,
	})
}

func (q *Query) FormatQuery(dst []byte, query string, params ...interface{}) []byte {
	params = append(params, q.model)
	if q.db != nil {
		return q.db.FormatQuery(dst, query, params...)
	}
	return formatter.Append(dst, query, params...)
}

func (q *Query) hasModel() bool {
	return !q.ignoreModel && q.model != nil
}

func (q *Query) hasTables() bool {
	return q.hasModel() || len(q.tables) > 0
}

func (q *Query) appendTableName(b []byte) []byte {
	return q.FormatQuery(b, string(q.model.Table().Name))
}

func (q *Query) appendTableNameWithAlias(b []byte) []byte {
	b = q.appendTableName(b)
	b = append(b, " AS "...)
	b = append(b, q.model.Table().Alias...)
	return b
}

func (q *Query) appendTables(b []byte) []byte {
	if q.hasModel() {
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

func (q *Query) appendFirstTable(b []byte) []byte {
	if q.hasModel() {
		return q.appendTableNameWithAlias(b)
	}
	if len(q.tables) > 0 {
		b = q.tables[0].AppendFormat(b, q)
	}
	return b
}

func (q *Query) hasOtherTables() bool {
	if q.hasModel() {
		return len(q.tables) > 0
	}
	return len(q.tables) > 1
}

func (q *Query) appendOtherTables(b []byte) []byte {
	tables := q.tables
	if !q.hasModel() {
		tables = tables[1:]
	}
	for i, f := range tables {
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
			b = append(b, ' ')
			b = f.AppendSep(b)
			b = append(b, ' ')
		}
		b = f.AppendFormat(b, q)
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

func (q *Query) appendWith(b []byte, count string) ([]byte, error) {
	var err error
	b = append(b, "WITH "...)
	for i, with := range q.with {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = types.AppendField(b, with.name, 1)
		b = append(b, " AS ("...)

		if count != "" {
			b, err = with.query.countSelectQuery("*").AppendQuery(b)
		} else {
			b, err = selectQuery{q: with.query}.AppendQuery(b)
		}
		if err != nil {
			return nil, err
		}

		b = append(b, ')')
	}
	b = append(b, ' ')
	return b, nil
}

//------------------------------------------------------------------------------

type wherePKQuery struct {
	*Query
}

func (wherePKQuery) AppendSep(b []byte) []byte {
	return append(b, "AND"...)
}

func (q wherePKQuery) AppendFormat(b []byte, f QueryFormatter) []byte {
	table := q.model.Table()
	return appendColumnAndValue(b, q.model.Value(), table, table.PKs)
}
