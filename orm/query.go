package orm

import (
	"context"
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

func (q *joinQuery) AppendOn(app *condAppender) {
	q.on = append(q.on, app)
}

type Query struct {
	ctx       context.Context
	db        DB
	fmter     QueryFormatter
	stickyErr error

	model         TableModel
	implicitModel bool
	deleted       bool

	with         []withQuery
	tables       []FormatAppender
	columns      []FormatAppender
	set          []FormatAppender
	modelValues  map[string]*queryParamsAppender
	where        []sepFormatAppender
	updWhere     []sepFormatAppender
	joins        []*joinQuery
	joinAppendOn func(app *condAppender)
	group        []FormatAppender
	having       []*queryParamsAppender
	order        []FormatAppender
	onConflict   *queryParamsAppender
	returning    []*queryParamsAppender
	limit        int
	offset       int
	selFor       *queryParamsAppender
}

func NewQuery(db DB, model ...interface{}) *Query {
	return (&Query{}).DB(db).Model(model...)
}

func NewQueryContext(c context.Context, db DB, model ...interface{}) *Query {
	return NewQuery(db, model...).Context(c)
}

// New returns new zero Query binded to the current db.
func (q *Query) New() *Query {
	cp := &Query{
		ctx:           q.ctx,
		db:            q.db,
		model:         q.model,
		implicitModel: true,
		deleted:       q.deleted,
	}
	return cp
}

// Copy returns copy of the Query.
func (q *Query) Copy() *Query {
	var modelValues map[string]*queryParamsAppender
	if len(q.modelValues) > 0 {
		modelValues = make(map[string]*queryParamsAppender, len(q.modelValues))
		for k, v := range q.modelValues {
			modelValues[k] = v
		}
	}

	copy := &Query{
		ctx:       q.ctx,
		db:        q.db,
		fmter:     q.fmter,
		stickyErr: q.stickyErr,

		model:         q.model,
		implicitModel: q.implicitModel,
		deleted:       q.deleted,

		with:        q.with[:len(q.with):len(q.with)],
		tables:      q.tables[:len(q.tables):len(q.tables)],
		columns:     q.columns[:len(q.columns):len(q.columns)],
		set:         q.set[:len(q.set):len(q.set)],
		modelValues: modelValues,
		where:       q.where[:len(q.where):len(q.where)],
		updWhere:    q.updWhere[:len(q.updWhere):len(q.updWhere)],
		joins:       q.joins[:len(q.joins):len(q.joins)],
		group:       q.group[:len(q.group):len(q.group)],
		having:      q.having[:len(q.having):len(q.having)],
		order:       q.order[:len(q.order):len(q.order)],
		onConflict:  q.onConflict,
		returning:   q.returning[:len(q.returning):len(q.returning)],
		limit:       q.limit,
		offset:      q.offset,
		selFor:      q.selFor,
	}

	return copy
}

func (q *Query) err(err error) *Query {
	if q.stickyErr == nil {
		q.stickyErr = err
	}
	return q
}

func (q *Query) Context(c context.Context) *Query {
	q.ctx = c
	return q
}

func (q *Query) DB(db DB) *Query {
	q.db = db
	return q
}

func (q *Query) Formatter(fmter QueryFormatter) *Query {
	q.fmter = fmter
	return q
}

func (q *Query) formatter() QueryFormatter {
	if q.fmter != nil {
		return q.fmter
	}
	if q.db != nil {
		return q.db
	}
	return defaultFmter
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
	q.implicitModel = false
	return q
}

func (q *Query) GetModel() TableModel {
	return q.model
}

func (q *Query) softDelete() bool {
	if q.model != nil {
		return q.model.Table().SoftDeleteField != nil
	}
	return false
}

// Deleted adds `WHERE deleted_at IS NOT NULL` clause for soft deleted models.
func (q *Query) Deleted() *Query {
	if q.model != nil {
		if err := q.model.Table().mustSoftDelete(); err != nil {
			return q.err(err)
		}
	}
	q.deleted = true
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

// Column adds a column to the Query quoting it according to PostgreSQL rules. Does not expand params like ?TableAlias etc.
// ColumnExpr can be used to bypass quoting restriction or for params expansion. Column name can be:
//   - column_name,
//   - table_alias.column_name,
//   - table_alias.*.
func (q *Query) Column(columns ...string) *Query {
	for _, column := range columns {
		if column == "_" {
			if q.columns == nil {
				q.columns = make([]FormatAppender, 0)
			}
			continue
		}

		if q.model != nil {
			if j := q.model.Join(column, nil); j != nil {
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

// ExcludeColumn excludes a column from the list of to be selected columns.
func (q *Query) ExcludeColumn(columns ...string) *Query {
	if q.columns == nil {
		for _, f := range q.model.Table().Fields {
			q.columns = append(q.columns, fieldAppender{f.SQLName})
		}
	}

	for _, col := range columns {
		if !q.excludeColumn(col) {
			return q.err(fmt.Errorf("pg: can't find column=%q", col))
		}
	}
	return q
}

func (q *Query) excludeColumn(column string) bool {
	for i := 0; i < len(q.columns); i++ {
		app, ok := q.columns[i].(fieldAppender)
		if ok && app.field == column {
			q.columns = append(q.columns[:i], q.columns[i+1:]...)
			return true
		}
	}
	return false
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

// Relation adds a relation to the query. Relation name can be:
//   - RelationName to select all columns,
//   - RelationName.column_name,
//   - RelationName._ to join relation without selecting relation columns.
func (q *Query) Relation(name string, apply ...func(*Query) (*Query, error)) *Query {
	var fn func(*Query) (*Query, error)
	if len(apply) == 1 {
		fn = apply[0]
	} else if len(apply) > 1 {
		panic("only one apply function is supported")
	}

	join := q.model.Join(name, fn)
	if join == nil {
		return q.err(fmt.Errorf("%s does not have relation=%q",
			q.model.Table(), name))
	}

	if fn == nil {
		return q
	}

	switch join.Rel.Type {
	case HasOneRelation, BelongsToRelation:
		q.joinAppendOn = join.AppendOn
		return q.Apply(fn)
	default:
		q.joinAppendOn = nil
		return q
	}
}

func (q *Query) Set(set string, params ...interface{}) *Query {
	q.set = append(q.set, &queryParamsAppender{set, params})
	return q
}

// Value overwrites model value for the column in INSERT and UPDATE queries.
func (q *Query) Value(column string, value string, params ...interface{}) *Query {
	if !q.hasModel() {
		q.err(errModelNil)
		return q
	}

	table := q.model.Table()
	if _, ok := table.FieldsMap[column]; !ok {
		q.err(fmt.Errorf("%s does not have column=%q", table, column))
		return q
	}

	if q.modelValues == nil {
		q.modelValues = make(map[string]*queryParamsAppender)
	}
	q.modelValues[column] = &queryParamsAppender{value, params}
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
//    	WhereGroup(func(q *orm.Query) (*orm.Query, error) {
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
//    	WhereOrGroup(func(q *orm.Query) (*orm.Query, error) {
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

	if len(newq.where) == 0 {
		newq.where = saved
		return newq
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

// WherePK adds condition based on the model primary keys.
// Usually it is the same as:
//
//    Where("id = ?id")
func (q *Query) WherePK() *Query {
	if !q.hasModel() {
		q.err(errModelNil)
		return q
	}

	if err := q.model.Table().checkPKs(); err != nil {
		q.err(err)
		return q
	}

	q.where = append(q.where, wherePKQuery{q})
	return q
}

// WhereStruct generates conditions for the struct fields with non-zero values:
//    - Foo int - Where("foo = ?", strct.Foo)
//    - Foo []int - Where("foo = ANY(?)", pg.Array(strct.Foo))
//    - FooNEQ int - Where("foo != ?", strct.Foo)
//    - FooExclude int - Where("foo != ?", strct.Foo)
//    - FooGT int - Where("foo > ?", strct.Foo)
//    - FooGTE int - Where("foo >= ?", strct.Foo)
//    - FooLT int - Where("foo < ?", strct.Foo)
//    - FooLTE int - Where("foo <= ?", strct.Foo)
//
// urlvalues.Decode can be used to decode url.Values into the struct.
//
// Following field tags are recognized:
//    - pg:"-" - field is ignored.
//    - pg:",nowhere" - field is decoded but is ignored by WhereStruct.
//    - pg:",nodecode" - field is not decoded but is used by WhereStruct.
//    - pg:",required" - condition is added for zero values as well.
func (q *Query) WhereStruct(strct interface{}) *Query {
	q.where = append(q.where, newStructFilter(strct))
	return q
}

func (q *Query) Join(join string, params ...interface{}) *Query {
	j := &joinQuery{
		join: &queryParamsAppender{join, params},
	}
	q.joins = append(q.joins, j)
	q.joinAppendOn = j.AppendOn
	return q
}

// JoinOn appends join condition to the last join.
func (q *Query) JoinOn(condition string, params ...interface{}) *Query {
	if q.joinAppendOn == nil {
		q.err(errors.New("pg: no joins to apply JoinOn"))
		return q
	}
	q.joinAppendOn(&condAppender{
		sep:    " AND ",
		cond:   condition,
		params: params,
	})
	return q
}

func (q *Query) JoinOnOr(condition string, params ...interface{}) *Query {
	if q.joinAppendOn == nil {
		q.err(errors.New("pg: no joins to apply JoinOn"))
		return q
	}
	q.joinAppendOn(&condAppender{
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

// Order adds sort order to the Query quoting column name. Does not expand params like ?TableAlias etc.
// OrderExpr can be used to bypass quoting restriction or for params expansion.
func (q *Query) Order(orders ...string) *Query {
loop:
	for _, order := range orders {
		if order == "" {
			continue
		}
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
	if order != "" {
		q.order = append(q.order, &queryParamsAppender{order, params})
	}
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
	_, err := q.db.QueryOneContext(
		q.ctx, Scan(&count), q.countSelectQuery("count(*)"), q.model)
	return count, err
}

func (q *Query) countSelectQuery(column string) *selectQuery {
	return &selectQuery{
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

	q, err = model.BeforeSelectQuery(q.ctx, q.db, q)
	if err != nil {
		return err
	}

	res, err := q.query(model, &selectQuery{q: q})
	if err != nil {
		return err
	}

	if res.RowsReturned() > 0 {
		if q.model != nil {
			if err := q.selectJoins(q.model.GetJoins()); err != nil {
				return err
			}
		}
		if err := model.AfterSelect(q.ctx, q.db); err != nil {
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
		return q.db.QueryOneContext(q.ctx, model, query, q.model)
	}
	return q.db.QueryContext(q.ctx, model, query, q.model)
}

// SelectAndCount runs Select and Count in two goroutines,
// waits for them to finish and returns the result. If query limit is -1
// it does not select any data and only counts the results.
func (q *Query) SelectAndCount(values ...interface{}) (count int, firstErr error) {
	if q.stickyErr != nil {
		return 0, q.stickyErr
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	if q.limit >= 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := q.Select(values...)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		count, err = q.Count()
		if err != nil {
			mu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
		}
	}()

	wg.Wait()
	return count, firstErr
}

// SelectAndCountEstimate runs Select and CountEstimate in two goroutines,
// waits for them to finish and returns the result. If query limit is -1
// it does not select any data and only counts the results.
func (q *Query) SelectAndCountEstimate(threshold int, values ...interface{}) (count int, firstErr error) {
	if q.stickyErr != nil {
		return 0, q.stickyErr
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	if q.limit >= 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := q.Select(values...)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		count, err = q.CountEstimate(threshold)
		if err != nil {
			mu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
		}
	}()

	wg.Wait()
	return count, firstErr
}

// ForEach calls the function for each row returned by the query
// without loading all rows into the memory.
// Function accepts a struct, pointer to a struct, orm.Model,
// or values for columns in a row. Function must return an error.
func (q *Query) ForEach(fn interface{}) error {
	m := newFuncModel(fn)
	return q.Select(m)
}

func (q *Query) forEachHasOneJoin(fn func(*join)) {
	if q.model == nil {
		return
	}
	q._forEachHasOneJoin(fn, q.model.GetJoins())
}

func (q *Query) forEachHasManyJoin(fn func(*join)) {
	if q.model == nil {
		return
	}
	q._forEachHasManyJoin(fn, q.model.GetJoins())
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

func (q *Query) _forEachHasManyJoin(fn func(*join), joins []join) {
	for i := range joins {
		j := &joins[i]
		switch j.Rel.Type {
		case HasManyRelation:
			fn(j)
			q._forEachHasManyJoin(fn, j.JoinModel.GetJoins())
		}
	}
}

func (q *Query) selectJoins(joins []join) error {
	var err error
	for i := range joins {
		j := &joins[i]
		if j.Rel.Type == HasOneRelation || j.Rel.Type == BelongsToRelation {
			err = q.selectJoins(j.JoinModel.GetJoins())
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
		err = q.model.BeforeInsert(q.ctx, q.db)
		if err != nil {
			return nil, err
		}
	}

	query := &insertQuery{q: q}
	res, err := q.returningQuery(model, query)
	if err != nil {
		return nil, err
	}

	if q.model != nil {
		err = q.model.AfterInsert(q.ctx, q.db)
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

	var insertq *Query

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

		if insertq == nil {
			insertq = q
			if len(insertq.columns) > 0 {
				insertq = insertq.Copy()
				insertq.columns = nil
			}
		}

		res, err := insertq.Insert(values...)
		if err != nil {
			insertErr = err
			if err == internal.ErrNoRows {
				continue
			}
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
		err = q.model.BeforeUpdate(q.ctx, q.db)
		if err != nil {
			return nil, err
		}
	}

	query := &updateQuery{q: q, omitZero: omitZero}
	res, err := q.returningQuery(model, query)
	if err != nil {
		return nil, err
	}

	if q.model != nil {
		err = q.model.AfterUpdate(q.ctx, q.db)
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (q *Query) returningQuery(model Model, query interface{}) (Result, error) {
	if len(q.returning) == 0 {
		return q.db.QueryContext(q.ctx, model, query, q.model)
	}
	if _, ok := model.(useQueryOne); ok {
		return q.db.QueryOneContext(q.ctx, model, query, q.model)
	}
	return q.db.QueryContext(q.ctx, model, query, q.model)
}

// Delete deletes the model. When model has deleted_at column the row
// is soft deleted instead.
func (q *Query) Delete(values ...interface{}) (Result, error) {
	if q.hasModel() {
		table := q.model.Table()
		if table.SoftDeleteField != nil {
			q.model.setSoftDeleteField()
			columns := q.columns
			q.columns = nil
			res, err := q.Column(table.SoftDeleteField.SQLName).Update(values...)
			q.columns = columns
			return res, err
		}
	}
	return q.ForceDelete(values...)
}

// Delete forces delete of the model with deleted_at column.
func (q *Query) ForceDelete(values ...interface{}) (Result, error) {
	if q.stickyErr != nil {
		return nil, q.stickyErr
	}
	if q.model == nil {
		return nil, errModelNil
	}
	q.deleted = true

	model, err := q.newModel(values...)
	if err != nil {
		return nil, err
	}

	if q.model != nil {
		err = q.model.BeforeDelete(q.ctx, q.db)
		if err != nil {
			return nil, err
		}
	}

	res, err := q.returningQuery(model, &deleteQuery{q: q})
	if err != nil {
		return nil, err
	}

	if q.model != nil {
		err = q.model.AfterDelete(q.ctx, q.db)
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (q *Query) CreateTable(opt *CreateTableOptions) error {
	_, err := q.db.ExecContext(q.ctx, &createTableQuery{
		q:   q,
		opt: opt,
	})
	return err
}

func (q *Query) DropTable(opt *DropTableOptions) error {
	_, err := q.db.ExecContext(q.ctx, &dropTableQuery{
		q:   q,
		opt: opt,
	})
	return err
}

// Exec is an alias for DB.Exec.
func (q *Query) Exec(query interface{}, params ...interface{}) (Result, error) {
	params = append(params, q.model)
	return q.db.ExecContext(q.ctx, query, params...)
}

// ExecOne is an alias for DB.ExecOne.
func (q *Query) ExecOne(query interface{}, params ...interface{}) (Result, error) {
	params = append(params, q.model)
	return q.db.ExecOneContext(q.ctx, query, params...)
}

// Query is an alias for DB.Query.
func (q *Query) Query(model, query interface{}, params ...interface{}) (Result, error) {
	params = append(params, q.model)
	return q.db.QueryContext(q.ctx, model, query, params...)
}

// QueryOne is an alias for DB.QueryOne.
func (q *Query) QueryOne(model, query interface{}, params ...interface{}) (Result, error) {
	params = append(params, q.model)
	return q.db.QueryOneContext(q.ctx, model, query, params...)
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
	return q.formatter().FormatQuery(b, query, params...)
}

var _ FormatAppender = (*Query)(nil)

func (q *Query) AppendFormat(b []byte, fmter QueryFormatter) []byte {
	cp := q.Copy().Formatter(fmter)
	bb, err := (&selectQuery{q: cp}).AppendQuery(b)
	if err != nil {
		q.err(err)
		return types.AppendError(b, err)
	}
	return bb
}

// Exists returns true or false depending if there are any rows matching the query.
func (q *Query) Exists() (bool, error) {
	cp := q.Copy() // copy to not change original query
	cp.columns = []FormatAppender{Q("1")}
	cp.order = nil
	cp.limit = 1
	res, err := q.db.ExecContext(q.ctx, &selectQuery{q: cp})
	if err != nil {
		return false, err
	}
	return res.RowsAffected() > 0, nil
}

func (q *Query) hasModel() bool {
	return q.model != nil && !q.model.IsNil()
}

func (q *Query) hasExplicitModel() bool {
	return q.model != nil && !q.implicitModel
}

func (q *Query) modelHasTableName() bool {
	return q.hasExplicitModel() && q.model.Table().FullName != ""
}

func (q *Query) modelHasTableAlias() bool {
	return q.hasExplicitModel() && q.model.Table().Alias != ""
}

func (q *Query) hasTables() bool {
	return q.modelHasTableName() || len(q.tables) > 0
}

func (q *Query) appendFirstTable(b []byte) []byte {
	if q.modelHasTableName() {
		return q.FormatQuery(b, string(q.model.Table().FullName))
	}
	if len(q.tables) > 0 {
		b = q.tables[0].AppendFormat(b, q)
	}
	return b
}

func (q *Query) appendFirstTableWithAlias(b []byte) []byte {
	if q.modelHasTableName() {
		table := q.model.Table()
		b = q.FormatQuery(b, string(table.FullName))
		b = append(b, " AS "...)
		b = append(b, table.Alias...)
		return b
	}

	if len(q.tables) > 0 {
		b = q.tables[0].AppendFormat(b, q)
		if q.modelHasTableAlias() {
			b = append(b, " AS "...)
			b = append(b, q.model.Table().Alias...)
		}
	}

	return b
}

func (q *Query) hasMultiTables() bool {
	if q.modelHasTableName() {
		return len(q.tables) > 0
	}
	return len(q.tables) > 1
}

func (q *Query) appendOtherTables(b []byte) []byte {
	tables := q.tables
	if !q.modelHasTableName() {
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

func (q *Query) hasWhere() bool {
	return len(q.where) > 0 || q.softDelete()
}

func (q *Query) mustAppendWhere(b []byte) ([]byte, error) {
	if q.hasWhere() {
		b = q.appendWhere(b)
		return b, nil
	}

	err := errors.New(
		"pg: Update and Delete queries require Where clause (try WherePK)")
	return nil, err
}

func (q *Query) appendWhere(b []byte) []byte {
	b = q._appendWhere(b, q.where)
	if q.softDelete() {
		if len(q.where) > 0 {
			b = append(b, " AND "...)
		}
		b = append(b, q.model.Table().Alias...)
		b = q.appendSoftDelete(b)
	}
	return b
}

func (q *Query) appendSoftDelete(b []byte) []byte {
	b = append(b, '.')
	b = append(b, q.model.Table().SoftDeleteField.Column...)
	if q.deleted {
		b = append(b, " IS NOT NULL"...)
	} else {
		b = append(b, " IS NULL"...)
	}
	return b
}

func (q *Query) appendUpdWhere(b []byte) []byte {
	return q._appendWhere(b, q.updWhere)
}

func (q *Query) _appendWhere(b []byte, where []sepFormatAppender) []byte {
	for i, f := range where {
		start := len(b)
		if i > 0 {
			b = f.AppendSep(b)
		}
		before := len(b)
		b = f.AppendFormat(b, q)
		if len(b) == before {
			b = b[:start]
		}
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

func (q *Query) appendWith(b []byte) []byte {
	b = append(b, "WITH "...)
	for i, with := range q.with {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = types.AppendField(b, with.name, 1)
		b = append(b, " AS ("...)
		b = with.query.AppendFormat(b, q)
		b = append(b, ')')
	}
	b = append(b, ' ')
	return b
}

func (q *Query) isSliceModelWithData() bool {
	if !q.hasModel() {
		return false
	}
	m, ok := q.model.(*sliceTableModel)
	return ok && m.sliceLen > 0
}

//------------------------------------------------------------------------------

type wherePKQuery struct {
	*Query
}

func (wherePKQuery) AppendSep(b []byte) []byte {
	return append(b, " AND "...)
}

func (q wherePKQuery) AppendFormat(b []byte, fmter QueryFormatter) []byte {
	table := q.model.Table()
	value := q.model.Value()
	if q.model.Kind() == reflect.Slice {
		return appendColumnAndSliceValue(fmter, b, value, table.Alias, table.PKs)
	} else {
		return appendColumnAndValue(fmter, b, value, table.Alias, table.PKs)
	}
}

func appendColumnAndValue(
	fmter QueryFormatter, b []byte, v reflect.Value, alias types.Q, fields []*Field,
) []byte {
	isPlaceholder := isPlaceholderFormatter(fmter)
	for i, f := range fields {
		if i > 0 {
			b = append(b, " AND "...)
		}
		b = append(b, alias...)
		b = append(b, '.')
		b = append(b, f.Column...)
		b = append(b, " = "...)
		if isPlaceholder {
			b = append(b, '?')
		} else {
			b = f.AppendValue(b, v, 1)
		}
	}
	return b
}

func appendColumnAndSliceValue(
	fmter QueryFormatter, b []byte, slice reflect.Value, alias types.Q, fields []*Field,
) []byte {
	if len(fields) > 1 {
		b = append(b, '(')
	}
	b = appendColumns(b, alias, fields)
	if len(fields) > 1 {
		b = append(b, ')')
	}

	b = append(b, " IN ("...)

	isPlaceholder := isPlaceholderFormatter(fmter)
	for i := 0; i < slice.Len(); i++ {
		if i > 0 {
			b = append(b, ", "...)
		}

		el := indirect(slice.Index(i))

		if len(fields) > 1 {
			b = append(b, '(')
		}
		for i, f := range fields {
			if i > 0 {
				b = append(b, ", "...)
			}
			if isPlaceholder {
				b = append(b, '?')
			} else {
				b = f.AppendValue(b, el, 1)
			}
		}
		if len(fields) > 1 {
			b = append(b, ')')
		}
	}

	b = append(b, ')')

	return b
}
