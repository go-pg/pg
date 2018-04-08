package orm

import (
	"errors"
	"fmt"
	"io"
	"reflect"
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

type joinQuery struct {
	join *queryParamsAppender
	on   []*condAppender
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
	values     map[string]*queryParamsAppender
	where      []sepFormatAppender
	updWhere   []sepFormatAppender
	joins      []*joinQuery
	group      []FormatAppender
	having     []*queryParamsAppender
	order      []FormatAppender
	onConflict *queryParamsAppender
	returning  []*queryParamsAppender
	limit      int
	offset     int
	selFor     FormatAppender
}

var _ queryAppender = (*Query)(nil)

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

func (q *Query) AppendQuery(b []byte) ([]byte, error) {
	return selectQuery{q: q}.AppendQuery(b)
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
		updWhere:   q.updWhere[:len(q.updWhere):len(q.updWhere)],
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
	q.tables = append(q.tables, &queryParamsAppender{expr, params})
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
	q.columns = append(q.columns, &queryParamsAppender{expr, params})
	return q
}

func (q *Query) getFields() ([]*Field, error) {
	return q._getFields(false)
}

func (q *Query) getDataFields() ([]*Field, error) {
	return q._getFields(true)
}

func (q *Query) _getFields(omitPKs bool) ([]*Field, error) {
	table := q.model.Table()

	var columns []*Field
	for _, col := range q.columns {
		f, ok := col.(fieldAppender)
		if !ok {
			continue
		}

		field, err := table.GetField(f.field)
		if err != nil {
			return nil, err
		}

		if omitPKs && field.HasFlag(PrimaryKeyFlag) {
			continue
		}

		columns = append(columns, field)
	}
	return columns, nil
}

func (q *Query) Relation(name string, apply ...func(*Query) (*Query, error)) *Query {
	var fn func(*Query) (*Query, error)
	if len(apply) == 1 {
		fn = apply[0]
	} else if len(apply) > 1 {
		panic("only one apply function is supported")
	}
	_, join := q.model.Join(name, fn)
	if join == nil {
		return q.err(fmt.Errorf(
			"model=%s does not have relation=%s",
			q.model.Table().Type.Name(), name,
		))
	}
	return q
}

func (q *Query) Set(set string, params ...interface{}) *Query {
	q.set = append(q.set, &queryParamsAppender{set, params})
	return q
}

func (q *Query) Value(column string, value string, params ...interface{}) *Query {
	if q.values == nil {
		q.values = make(map[string]*queryParamsAppender)
	}
	q.values[column] = &queryParamsAppender{value, params}
	return q
}

func (q *Query) Where(condition string, params ...interface{}) *Query {
	q.addWhere(&condAppender{
		sep:    " AND ",
		cond:   condition,
		params: params,
	})
	return q
}

func (q *Query) WhereOr(condition string, params ...interface{}) *Query {
	q.addWhere(&condAppender{
		sep:    " OR ",
		cond:   condition,
		params: params,
	})
	return q
}

// WhereGroup encloses conditions added in the function in parentheses.
//
//    q.Where("TRUE").
//    	WhereGroup(func(q *orm.Query) (*orm.Query, error)) {
//    		q = q.WhereOr("FALSE").WhereOr("TRUE").
//    		return q, nil
//    	})
//
// generates
//
//    WHERE TRUE AND (FALSE OR TRUE)
func (q *Query) WhereGroup(fn func(*Query) (*Query, error)) *Query {
	return q.whereGroup(" AND ", fn)
}

// WhereOrGroup encloses conditions added in the function in parentheses.
//
//    q.Where("TRUE").
//    	WhereOrGroup(func(q *orm.Query) (*orm.Query, error)) {
//    		q = q.Where("FALSE").Where("TRUE").
//    		return q, nil
//    	})
//
// generates
//
//    WHERE TRUE OR (FALSE AND TRUE)
func (q *Query) WhereOrGroup(fn func(*Query) (*Query, error)) *Query {
	return q.whereGroup(" OR ", fn)
}

func (q *Query) whereGroup(conj string, fn func(*Query) (*Query, error)) *Query {
	saved := q.where
	q.where = nil

	newq, err := fn(q)
	if err != nil {
		q.err(err)
		return q
	}

	f := &condGroupAppender{
		sep:  conj,
		cond: newq.where,
	}
	newq.where = saved
	newq.addWhere(f)

	return newq
}

// WhereIn is a shortcut for Where and pg.In to work with IN operator:
//
//    WhereIn("id IN (?)", 1, 2, 3)
func (q *Query) WhereIn(where string, values ...interface{}) *Query {
	return q.Where(where, types.InSlice(values))
}

func (q *Query) addWhere(f sepFormatAppender) {
	if q.onConflictDoUpdate() {
		q.updWhere = append(q.updWhere, f)
	} else {
		q.where = append(q.where, f)
	}
}

// WherePK adds condition based on the model primary key.
// Typically it is the same as:
//
//    Where("id = ?id")
func (q *Query) WherePK() *Query {
	if !q.hasModel() {
		q.stickyErr = errors.New("pg: Model(nil)")
		return q
	}
	if q.model.Kind() == reflect.Slice {
		q.stickyErr = errors.New("pg: WherePK requires struct Model")
		return q
	}
	if err := q.model.Table().checkPKs(); err != nil {
		q.stickyErr = err
		return q
	}
	q.where = append(q.where, wherePKQuery{q})
	return q
}

func (q *Query) Join(join string, params ...interface{}) *Query {
	q.joins = append(q.joins, &joinQuery{
		join: &queryParamsAppender{join, params},
	})
	return q
}

// JoinOn appends join condition to the last join.
func (q *Query) JoinOn(condition string, params ...interface{}) *Query {
	j := q.joins[len(q.joins)-1]
	j.on = append(j.on, &condAppender{
		sep:    " AND ",
		cond:   condition,
		params: params,
	})
	return q
}

func (q *Query) JoinOnOr(condition string, params ...interface{}) *Query {
	j := q.joins[len(q.joins)-1]
	j.on = append(j.on, &condAppender{
		sep:    " OR ",
		cond:   condition,
		params: params,
	})
	return q
}

func (q *Query) Group(columns ...string) *Query {
	for _, column := range columns {
		q.group = append(q.group, fieldAppender{column})
	}
	return q
}

func (q *Query) GroupExpr(group string, params ...interface{}) *Query {
	q.group = append(q.group, &queryParamsAppender{group, params})
	return q
}

func (q *Query) Having(having string, params ...interface{}) *Query {
	q.having = append(q.having, &queryParamsAppender{having, params})
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
			switch internal.UpperString(sort) {
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
	q.order = append(q.order, &queryParamsAppender{order, params})
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
	q.onConflict = &queryParamsAppender{s, params}
	return q
}

func (q *Query) onConflictDoUpdate() bool {
	return q.onConflict != nil &&
		strings.HasSuffix(internal.UpperString(q.onConflict.query), "DO UPDATE")
}

func (q *Query) Returning(s string, params ...interface{}) *Query {
	q.returning = append(q.returning, &queryParamsAppender{s, params})
	return q
}

func (q *Query) For(s string, params ...interface{}) *Query {
	q.selFor = &queryParamsAppender{s, params}
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
		q.countSelectQuery("count(*)"),
		q.model,
	)
	return count, err
}

func (q *Query) countSelectQuery(column string) selectQuery {
	return selectQuery{
		q:     q,
		count: column,
	}
}

// First sorts rows by primary key and selects the first row.
// It is a shortcut for:
//
//    q.OrderExpr("id ASC").Limit(1)
func (q *Query) First() error {
	err := q.model.Table().checkPKs()
	if err != nil {
		return err
	}

	b := appendColumns(nil, q.model.Table().Alias, q.model.Table().PKs)
	return q.OrderExpr(internal.BytesToString(b)).Limit(1).Select()
}

// Last sorts rows by primary key and selects the last row.
// It is a shortcut for:
//
//    q.OrderExpr("id DESC").Limit(1)
func (q *Query) Last() error {
	err := q.model.Table().checkPKs()
	if err != nil {
		return err
	}

	// TODO: fix for multi columns
	b := appendColumns(nil, q.model.Table().Alias, q.model.Table().PKs)
	b = append(b, " DESC"...)
	return q.OrderExpr(internal.BytesToString(b)).Limit(1).Select()
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
			if err := q.selectJoins(q.model.GetJoins()); err != nil {
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

func (q *Query) selectJoins(joins []join) error {
	var err error
	for i := range joins {
		j := &joins[i]
		if j.Rel.Type == HasOneRelation || j.Rel.Type == BelongsToRelation {
			err = q.selectJoins(j.JoinModel.GetJoins())
		} else {
			err = j.Select(q.db)
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

	if q.model != nil && q.model.Table().HasFlag(BeforeInsertHookFlag) {
		err = q.model.BeforeInsert(q.db)
		if err != nil {
			return nil, err
		}
	}

	res, err := q.db.Query(model, insertQuery{q: q}, q.model)
	if err != nil {
		return nil, err
	}

	if q.model != nil {
		err = q.model.AfterInsert(q.db)
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

// SelectOrInsert selects the model inserting one if it does not exist.
// It returns true when model was inserted.
func (q *Query) SelectOrInsert(values ...interface{}) (inserted bool, _ error) {
	if q.stickyErr != nil {
		return false, q.stickyErr
	}

	insertq := q
	if len(insertq.columns) > 0 {
		insertq = insertq.Copy()
		insertq.columns = nil
	}

	var insertErr error
	for i := 0; i < 5; i++ {
		if i >= 2 {
			time.Sleep(internal.RetryBackoff(i-2, 250*time.Millisecond, 5*time.Second))
		}

		err := q.Select(values...)
		if err == nil {
			return false, nil
		}
		if err != internal.ErrNoRows {
			return false, err
		}

		res, err := insertq.Insert(values...)
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

	err := fmt.Errorf(
		"pg: SelectOrInsert: select returns no rows (insert fails with err=%q)",
		insertErr,
	)
	return false, err
}

// Update updates the model.
func (q *Query) Update(scan ...interface{}) (Result, error) {
	return q.update(scan, false)
}

// Update updates the model omitting null columns.
func (q *Query) UpdateNotNull(scan ...interface{}) (Result, error) {
	return q.update(scan, true)
}

func (q *Query) update(scan []interface{}, omitZero bool) (Result, error) {
	if q.stickyErr != nil {
		return nil, q.stickyErr
	}

	model, err := q.newModel(scan...)
	if err != nil {
		return nil, err
	}

	if q.model != nil {
		err = q.model.BeforeUpdate(q.db)
		if err != nil {
			return nil, err
		}
	}

	res, err := q.db.Query(model, updateQuery{q: q, omitZero: omitZero}, q.model)
	if err != nil {
		return nil, err
	}

	if q.model != nil {
		err = q.model.AfterUpdate(q.db)
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

// Delete deletes the model.
func (q *Query) Delete(values ...interface{}) (Result, error) {
	if q.stickyErr != nil {
		return nil, q.stickyErr
	}

	model, err := q.newModel(values...)
	if err != nil {
		return nil, err
	}

	if q.model != nil {
		err = q.model.BeforeDelete(q.db)
		if err != nil {
			return nil, err
		}
	}

	res, err := q.db.Query(model, deleteQuery{q}, q.model)
	if err != nil {
		return nil, err
	}

	if q.model != nil {
		err = q.model.AfterDelete(q.db)
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

// Exec is an alias for DB.Exec.
func (q *Query) Exec(query interface{}, params ...interface{}) (Result, error) {
	params = append(params, q.model)
	return q.db.Exec(query, params...)
}

// ExecOne is an alias for DB.ExecOne.
func (q *Query) ExecOne(query interface{}, params ...interface{}) (Result, error) {
	params = append(params, q.model)
	return q.db.ExecOne(query, params...)
}

// Query is an alias for DB.Query.
func (q *Query) Query(model, query interface{}, params ...interface{}) (Result, error) {
	params = append(params, q.model)
	return q.db.Query(model, query, params...)
}

// QueryOne is an alias for DB.QueryOne.
func (q *Query) QueryOne(model, query interface{}, params ...interface{}) (Result, error) {
	params = append(params, q.model)
	return q.db.QueryOne(model, query, params...)
}

// CopyFrom is an alias from DB.CopyFrom.
func (q *Query) CopyFrom(r io.Reader, query interface{}, params ...interface{}) (Result, error) {
	params = append(params, q.model)
	return q.db.CopyFrom(r, query, params...)
}

// CopyTo is an alias from DB.CopyTo.
func (q *Query) CopyTo(w io.Writer, query interface{}, params ...interface{}) (Result, error) {
	params = append(params, q.model)
	return q.db.CopyTo(w, query, params...)
}

func (q *Query) FormatQuery(b []byte, query string, params ...interface{}) []byte {
	params = append(params, q.model)
	if q.db != nil {
		return q.db.FormatQuery(b, query, params...)
	}
	return formatter.Append(b, query, params...)
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

func (q *Query) appendFirstTable(b []byte) []byte {
	if q.hasModel() {
		return q.appendTableName(b)
	}
	if len(q.tables) > 0 {
		b = q.tables[0].AppendFormat(b, q)
	}
	return b
}

func (q *Query) appendFirstTableWithAlias(b []byte) []byte {
	if q.hasModel() {
		return q.appendTableNameWithAlias(b)
	}
	if len(q.tables) > 0 {
		b = q.tables[0].AppendFormat(b, q)
	}
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

func (q *Query) appendColumns(b []byte) []byte {
	for i, f := range q.columns {
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
	err := errors.New(
		"pg: Update and Delete queries require Where clause (try WherePK)")
	return nil, err
}

func (q *Query) appendWhere(b []byte) []byte {
	return q._appendWhere(b, q.where)
}

func (q *Query) appendUpdWhere(b []byte) []byte {
	return q._appendWhere(b, q.updWhere)
}

func (q *Query) _appendWhere(b []byte, where []sepFormatAppender) []byte {
	for i, f := range where {
		if i > 0 {
			b = f.AppendSep(b)
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

func (q *Query) appendWith(b []byte) ([]byte, error) {
	var err error
	b = append(b, "WITH "...)
	for i, with := range q.with {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = types.AppendField(b, with.name, 1)
		b = append(b, " AS ("...)

		b, err = selectQuery{q: with.query}.AppendQuery(b)
		if err != nil {
			return nil, err
		}

		b = append(b, ')')
	}
	b = append(b, ' ')
	return b, nil
}

func (q *Query) isSliceModel() bool {
	if !q.hasModel() {
		return false
	}
	return q.model.Kind() == reflect.Slice && q.model.Value().Len() > 0
}

//------------------------------------------------------------------------------

type wherePKQuery struct {
	*Query
}

func (wherePKQuery) AppendSep(b []byte) []byte {
	return append(b, " AND "...)
}

func (q wherePKQuery) AppendFormat(b []byte, f QueryFormatter) []byte {
	table := q.model.Table()
	value := q.model.Value()
	return appendColumnAndValue(b, value, table.Alias, table.PKs)
}

func appendColumnAndValue(b []byte, v reflect.Value, alias types.Q, fields []*Field) []byte {
	for i, f := range fields {
		if i > 0 {
			b = append(b, " AND "...)
		}
		b = append(b, alias...)
		b = append(b, '.')
		b = append(b, f.Column...)
		b = append(b, " = "...)
		b = f.AppendValue(b, v, 1)
	}
	return b
}

func appendColumnAndSliceValue(b []byte, slice reflect.Value, alias types.Q, fields []*Field) []byte {
	if slice.Len() == 0 {
		return append(b, "1 = 2"...)
	}

	if len(fields) > 1 {
		b = append(b, '(')
	}
	b = appendColumns(b, alias, fields)
	if len(fields) > 1 {
		b = append(b, ')')
	}

	b = append(b, " IN ("...)

	for i := 0; i < slice.Len(); i++ {
		if i > 0 {
			b = append(b, ", "...)
		}

		el := slice.Index(i)
		if el.Kind() == reflect.Interface {
			el = el.Elem()
		}

		if len(fields) > 1 {
			b = append(b, '(')
		}
		for i, f := range fields {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = f.AppendValue(b, el, 1)
		}
		if len(fields) > 1 {
			b = append(b, ')')
		}
	}
	b = append(b, ')')

	return b
}
