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

	"github.com/go-pg/pg/v11/internal"
	"github.com/go-pg/pg/v11/types"
)

type QueryOp string

const (
	SelectOp          QueryOp = "SELECT"
	InsertOp          QueryOp = "INSERT"
	UpdateOp          QueryOp = "UPDATE"
	DeleteOp          QueryOp = "DELETE"
	CreateTableOp     QueryOp = "CREATE TABLE"
	DropTableOp       QueryOp = "DROP TABLE"
	CreateCompositeOp QueryOp = "CREATE COMPOSITE"
	DropCompositeOp   QueryOp = "DROP COMPOSITE"
	ValuesOp          QueryOp = "VALUES"
)

type queryFlag uint8

const (
	implicitModelFlag queryFlag = 1 << iota
	wherePKFlag
	deletedFlag
	allWithDeletedFlag
)

type withQuery struct {
	name  string
	query QueryAppender
}

type columnValue struct {
	column string
	value  *SafeQueryAppender
}

type union struct {
	expr  string
	query *Query
}

type Query struct {
	db        DB
	stickyErr error

	model      Model
	tableModel TableModel
	flags      queryFlag

	with           []withQuery
	modelTableExpr *SafeQueryAppender
	tables         []QueryAppender
	distinctOn     []*SafeQueryAppender
	columns        []QueryAppender
	set            []QueryAppender
	modelValues    map[string]*SafeQueryAppender
	extraValues    []*columnValue

	where    []queryWithSepAppender
	updWhere []queryWithSepAppender

	group        []QueryAppender
	having       []*SafeQueryAppender
	union        []*union
	joins        []QueryAppender
	joinAppendOn func(app *condAppender)
	order        []QueryAppender
	limit        int
	offset       int
	selFor       *SafeQueryAppender

	onConflict *SafeQueryAppender
	returning  []*SafeQueryAppender
}

func NewQuery(db DB, model ...interface{}) *Query {
	q := new(Query)
	return q.DB(db).Model(model...)
}

// New returns new zero Query bound to the current db.
func (q *Query) New() *Query {
	clone := &Query{
		db: q.db,

		model:      q.model,
		tableModel: cloneTableModelJoins(q.tableModel),
	}
	return clone.withFlag(implicitModelFlag)
}

// Clone clones the Query.
func (q *Query) Clone() *Query {
	var modelValues map[string]*SafeQueryAppender
	if len(q.modelValues) > 0 {
		modelValues = make(map[string]*SafeQueryAppender, len(q.modelValues))
		for k, v := range q.modelValues {
			modelValues[k] = v
		}
	}

	clone := &Query{
		db:        q.db,
		stickyErr: q.stickyErr,

		model:      q.model,
		tableModel: cloneTableModelJoins(q.tableModel),
		flags:      q.flags,

		with:        q.with[:len(q.with):len(q.with)],
		tables:      q.tables[:len(q.tables):len(q.tables)],
		distinctOn:  q.distinctOn[:len(q.distinctOn):len(q.distinctOn)],
		columns:     q.columns[:len(q.columns):len(q.columns)],
		set:         q.set[:len(q.set):len(q.set)],
		modelValues: modelValues,
		extraValues: q.extraValues[:len(q.extraValues):len(q.extraValues)],
		where:       q.where[:len(q.where):len(q.where)],
		updWhere:    q.updWhere[:len(q.updWhere):len(q.updWhere)],
		joins:       q.joins[:len(q.joins):len(q.joins)],
		group:       q.group[:len(q.group):len(q.group)],
		having:      q.having[:len(q.having):len(q.having)],
		union:       q.union[:len(q.union):len(q.union)],
		order:       q.order[:len(q.order):len(q.order)],
		limit:       q.limit,
		offset:      q.offset,
		selFor:      q.selFor,

		onConflict: q.onConflict,
		returning:  q.returning[:len(q.returning):len(q.returning)],
	}

	return clone
}

func cloneTableModelJoins(tm TableModel) TableModel {
	switch tm := tm.(type) {
	case *structTableModel:
		if len(tm.joins) == 0 {
			return tm
		}
		clone := *tm
		clone.joins = clone.joins[:len(clone.joins):len(clone.joins)]
		return &clone
	case *sliceTableModel:
		if len(tm.joins) == 0 {
			return tm
		}
		clone := *tm
		clone.joins = clone.joins[:len(clone.joins):len(clone.joins)]
		return &clone
	}
	return tm
}

func (q *Query) Err(err error) *Query {
	if q.stickyErr == nil {
		q.stickyErr = err
	}
	return q
}

func (q *Query) hasFlag(flag queryFlag) bool {
	return hasFlag(q.flags, flag)
}

func hasFlag(flags, flag queryFlag) bool {
	return flags&flag != 0
}

func (q *Query) withFlag(flag queryFlag) *Query {
	q.flags |= flag
	return q
}

func (q *Query) withoutFlag(flag queryFlag) *Query {
	q.flags &= ^flag
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
		q.model, err = NewModel(model[0])
	case l > 1:
		q.model, err = NewModel(&model)
	default:
		panic("not reached")
	}
	if err != nil {
		q = q.Err(err)
	}

	q.tableModel, _ = q.model.(TableModel)

	return q.withoutFlag(implicitModelFlag)
}

func (q *Query) TableModel() TableModel {
	return q.tableModel
}

func (q *Query) isSoftDelete() bool {
	if q.tableModel != nil {
		return q.tableModel.Table().SoftDeleteField != nil && !q.hasFlag(allWithDeletedFlag)
	}
	return false
}

// Deleted adds `WHERE deleted_at IS NOT NULL` clause for soft deleted models.
func (q *Query) Deleted() *Query {
	if q.tableModel != nil {
		if err := q.tableModel.Table().mustSoftDelete(); err != nil {
			return q.Err(err)
		}
	}
	return q.withFlag(deletedFlag).withoutFlag(allWithDeletedFlag)
}

// AllWithDeleted changes query to return all rows including soft deleted ones.
func (q *Query) AllWithDeleted() *Query {
	if q.tableModel != nil {
		if err := q.tableModel.Table().mustSoftDelete(); err != nil {
			return q.Err(err)
		}
	}
	return q.withFlag(allWithDeletedFlag).withoutFlag(deletedFlag)
}

// With adds subq as common table expression with the given name.
func (q *Query) WithSelect(name string, subq *Query) *Query {
	return q._with(name, NewSelectQuery(subq))
}

func (q *Query) WithInsert(name string, subq *Query) *Query {
	return q._with(name, NewInsertQuery(subq))
}

func (q *Query) WithUpdate(name string, subq *Query) *Query {
	return q._with(name, NewUpdateQuery(subq, false))
}

func (q *Query) WithDelete(name string, subq *Query) *Query {
	return q._with(name, NewDeleteQuery(subq))
}

func (q *Query) WithValues(name string, subq *Query) *Query {
	return q._with(name, NewValuesQuery(subq))
}

func (q *Query) _with(name string, subq QueryAppender) *Query {
	q.with = append(q.with, withQuery{
		name:  name,
		query: subq,
	})
	return q
}

// WrapWith creates new Query and adds to it current query as
// common table expression with the given name.
func (q *Query) WrapWith(name string) *Query {
	wrapper := q.New()
	wrapper.with = q.with
	q.with = nil
	wrapper = wrapper.WithSelect(name, q)
	return wrapper
}

// Table adds a column name to the Query quoting it according to the PostgreSQL rules.
// The table name usually comes from a user and can't be trusted. If a table name
// is safe or you want to add an arbitrary SQL expression, use TableExpr.
func (q *Query) Table(tables ...string) *Query {
	for _, table := range tables {
		q.tables = append(q.tables, fieldAppender{table})
	}
	return q
}

// TableExpr adds an arbitrary table expression to the Query.
func (q *Query) TableExpr(expr string, params ...interface{}) *Query {
	q.tables = append(q.tables, SafeQuery(expr, params...))
	return q
}

func (q *Query) ModelTableExpr(expr string, params ...interface{}) *Query {
	q.modelTableExpr = SafeQuery(expr, params...)
	return q
}

func (q *Query) Distinct() *Query {
	q.distinctOn = make([]*SafeQueryAppender, 0)
	return q
}

func (q *Query) DistinctOn(expr string, params ...interface{}) *Query {
	q.distinctOn = append(q.distinctOn, SafeQuery(expr, params...))
	return q
}

// Column adds a column name to the Query quoting it according to the PostgreSQL rules.
// The column name usually comes from a user and can't be trusted. If a column name
// is safe or you want to add an arbitrary SQL expression, use ColumnExpr.
//
// go-pg recongnizes the following patterns:
//   - column_name,
//   - table_alias.column_name,
//   - table_alias.*.
func (q *Query) Column(columns ...string) *Query {
	for _, column := range columns {
		if column == "_" {
			if q.columns == nil {
				q.columns = make([]QueryAppender, 0)
			}
			continue
		}

		q.columns = append(q.columns, fieldAppender{column})
	}
	return q
}

// ColumnExpr adds an arbitrary column expression to the Query.
func (q *Query) ColumnExpr(expr string, params ...interface{}) *Query {
	q.columns = append(q.columns, SafeQuery(expr, params...))
	return q
}

// ExcludeColumn excludes a column from the list of to be selected columns.
func (q *Query) ExcludeColumn(columns ...string) *Query {
	if q.columns == nil {
		for _, f := range q.tableModel.Table().Fields {
			q.columns = append(q.columns, fieldAppender{f.SQLName})
		}
	}

	for _, col := range columns {
		if !q.excludeColumn(col) {
			return q.Err(fmt.Errorf("pg: can't find column=%q", col))
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
	table := q.tableModel.Table()

	if len(q.columns) == 0 {
		return table.Fields, nil
	}

	fields, err := q._getFields(table)
	if err != nil {
		return nil, err
	}

	fields = append(fields, table.PKs...)
	return fields, nil
}

func (q *Query) getDataFields() ([]*Field, error) {
	table := q.tableModel.Table()

	if len(q.columns) == 0 {
		return table.DataFields, nil
	}

	return q._getFields(table)
}

func (q *Query) _getFields(table *Table) ([]*Field, error) {
	columns := make([]*Field, 0, len(q.columns))
	for _, col := range q.columns {
		f, ok := col.(fieldAppender)
		if !ok {
			continue
		}

		field, err := table.GetField(f.field)
		if err != nil {
			return nil, err
		}

		if field.hasFlag(PrimaryKeyFlag) {
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
func (q *Query) Relation(name string, apply ...func(*Query) *Query) *Query {
	var fn func(*Query) *Query

	if len(apply) == 1 {
		fn = apply[0]
	} else if len(apply) > 1 {
		panic("only one apply function is supported")
	}

	join := q.tableModel.Join(name, fn)
	if join == nil {
		return q.Err(fmt.Errorf("%s does not have relation=%q",
			q.tableModel.Table(), name))
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
	q.set = append(q.set, SafeQuery(set, params...))
	return q
}

// Value overwrites model value for the column in INSERT and UPDATE queries.
func (q *Query) Value(column string, value string, params ...interface{}) *Query {
	if !q.hasTableModel() {
		q.Err(errModelNil)
		return q
	}

	table := q.tableModel.Table()
	if _, ok := table.FieldsMap[column]; ok {
		if q.modelValues == nil {
			q.modelValues = make(map[string]*SafeQueryAppender)
		}
		q.modelValues[column] = SafeQuery(value, params...)
	} else {
		q.extraValues = append(q.extraValues, &columnValue{
			column: column,
			value:  SafeQuery(value, params...),
		})
	}

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
//    	WhereGroup(func(q *orm.Query) *orm.Query {
//    		return q.WhereOr("FALSE").WhereOr("TRUE").
//    	})
//
// generates
//
//    WHERE TRUE AND (FALSE OR TRUE)
func (q *Query) WhereGroup(fn func(*Query) *Query) *Query {
	return q.whereGroup(" AND ", fn)
}

// WhereGroup encloses conditions added in the function in parentheses.
//
//    q.Where("TRUE").
//    	WhereNotGroup(func(q *orm.Query) *orm.Query {
//    		return q.WhereOr("FALSE").WhereOr("TRUE").
//    	})
//
// generates
//
//    WHERE TRUE AND NOT (FALSE OR TRUE)
func (q *Query) WhereNotGroup(fn func(*Query) *Query) *Query {
	return q.whereGroup(" AND NOT ", fn)
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
func (q *Query) WhereOrGroup(fn func(*Query) *Query) *Query {
	return q.whereGroup(" OR ", fn)
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
//    WHERE TRUE OR NOT (FALSE AND TRUE)
func (q *Query) WhereOrNotGroup(fn func(*Query) *Query) *Query {
	return q.whereGroup(" OR NOT ", fn)
}

func (q *Query) whereGroup(conj string, fn func(*Query) *Query) *Query {
	saved := q.where
	q.where = nil

	newq := fn(q)

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

// WhereIn is a shortcut for Where and pg.In.
func (q *Query) WhereIn(where string, slice interface{}) *Query {
	return q.Where(where, types.In(slice))
}

func (q *Query) addWhere(f queryWithSepAppender) {
	if q.onConflictDoUpdate() {
		q.updWhere = append(q.updWhere, f)
	} else {
		q.where = append(q.where, f)
	}
}

// WherePK adds conditions based on the model primary keys.
// Usually it is the same as:
//
//    Where("id = ?id")
func (q *Query) WherePK() *Query {
	q.withFlag(wherePKFlag)
	return q
}

func (q *Query) Join(join string, params ...interface{}) *Query {
	j := &joinQuery{
		join: SafeQuery(join, params...),
	}
	q.joins = append(q.joins, j)
	q.joinAppendOn = j.AppendOn
	return q
}

// JoinOn appends join condition to the last join.
func (q *Query) JoinOn(condition string, params ...interface{}) *Query {
	if q.joinAppendOn == nil {
		q.Err(errors.New("pg: no joins to apply JoinOn"))
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
		q.Err(errors.New("pg: no joins to apply JoinOn"))
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
	q.group = append(q.group, SafeQuery(group, params...))
	return q
}

func (q *Query) Having(having string, params ...interface{}) *Query {
	q.having = append(q.having, SafeQuery(having, params...))
	return q
}

func (q *Query) Union(other *Query) *Query {
	return q.addUnion(" UNION ", other)
}

func (q *Query) UnionAll(other *Query) *Query {
	return q.addUnion(" UNION ALL ", other)
}

func (q *Query) Intersect(other *Query) *Query {
	return q.addUnion(" INTERSECT ", other)
}

func (q *Query) IntersectAll(other *Query) *Query {
	return q.addUnion(" INTERSECT ALL ", other)
}

func (q *Query) Except(other *Query) *Query {
	return q.addUnion(" EXCEPT ", other)
}

func (q *Query) ExceptAll(other *Query) *Query {
	return q.addUnion(" EXCEPT ALL ", other)
}

func (q *Query) addUnion(expr string, other *Query) *Query {
	q.union = append(q.union, &union{
		expr:  expr,
		query: other,
	})
	return q
}

// Order adds a sorting order to the Query quoting it according to the PostgreSQL rules.
// The sorting order usually comes from a user and can't be trusted. If a sorting order
// is safe or you want to add an arbitrary SQL expression, use OrderExpr.
//
// Order recognizes the following patterns:
//   - column_name;
//   - column_name ASC;
//   - column_name DESC NULLS FIRST.
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
				q = q.OrderExpr("? ?", types.Ident(field), types.Safe(sort))
				continue loop
			}
		}

		q.order = append(q.order, fieldAppender{order})
	}
	return q
}

// Order adds an arbitrary sorting order to the Query.
func (q *Query) OrderExpr(order string, params ...interface{}) *Query {
	if order != "" {
		q.order = append(q.order, SafeQuery(order, params...))
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
	q.onConflict = SafeQuery(s, params...)
	return q
}

func (q *Query) onConflictDoUpdate() bool {
	return q.onConflict != nil &&
		strings.HasSuffix(internal.UpperString(q.onConflict.query), "DO UPDATE")
}

// Returning adds a RETURNING clause to the query.
//
// `Returning("NULL")` can be used to suppress default returning clause
// generated by go-pg for INSERT queries to get values for null columns.
func (q *Query) Returning(s string, params ...interface{}) *Query {
	q.returning = append(q.returning, SafeQuery(s, params...))
	return q
}

func (q *Query) For(s string, params ...interface{}) *Query {
	q.selFor = SafeQuery(s, params...)
	return q
}

// Apply calls the fn passing the Query as an argument.
func (q *Query) Apply(fn func(*Query) *Query) *Query {
	return fn(q)
}

// Count returns number of rows matching the query using count aggregate function.
func (q *Query) Count(ctx context.Context) (int, error) {
	if q.stickyErr != nil {
		return 0, q.stickyErr
	}

	var count int
	_, err := q.db.QueryOne(
		ctx, Scan(&count), q.countSelectQuery("count(*)"), q.tableModel)
	return count, err
}

func (q *Query) countSelectQuery(column string) *SelectQuery {
	return &SelectQuery{
		q:     q,
		count: column,
	}
}

// First sorts rows by primary key and selects the first row.
// It is a shortcut for:
//
//    q.OrderExpr("id ASC").Limit(1)
func (q *Query) First(ctx context.Context) error {
	table := q.tableModel.Table()

	if err := table.checkPKs(); err != nil {
		return err
	}

	b := appendColumns(nil, table.Alias, table.PKs)
	return q.OrderExpr(internal.BytesToString(b)).Limit(1).Select(ctx)
}

// Last sorts rows by primary key and selects the last row.
// It is a shortcut for:
//
//    q.OrderExpr("id DESC").Limit(1)
func (q *Query) Last(ctx context.Context) error {
	table := q.tableModel.Table()

	if err := table.checkPKs(); err != nil {
		return err
	}

	// TODO: fix for multi columns
	b := appendColumns(nil, table.Alias, table.PKs)
	b = append(b, " DESC"...)
	return q.OrderExpr(internal.BytesToString(b)).Limit(1).Select(ctx)
}

// Select selects the model.
func (q *Query) Select(ctx context.Context, values ...interface{}) error {
	if q.stickyErr != nil {
		return q.stickyErr
	}

	model, err := q.newModel(values)
	if err != nil {
		return err
	}

	res, err := q.query(ctx, model, NewSelectQuery(q))
	if err != nil {
		return err
	}

	if res.RowsReturned() > 0 {
		if q.tableModel != nil {
			if err := q.selectJoins(ctx, q.tableModel.GetJoins()); err != nil {
				return err
			}
		}
	}

	if err := model.AfterSelect(ctx); err != nil {
		return err
	}

	return nil
}

func (q *Query) newModel(values []interface{}) (Model, error) {
	if len(values) > 0 {
		return newScanModel(values)
	}
	return q.tableModel, nil
}

func (q *Query) query(ctx context.Context, model Model, query interface{}) (Result, error) {
	if _, ok := model.(useQueryOne); ok {
		return q.db.QueryOne(ctx, model, query, q.tableModel)
	}
	return q.db.Query(ctx, model, query, q.tableModel)
}

// SelectAndCount runs Select and Count in two goroutines,
// waits for them to finish and returns the result. If query limit is -1
// it does not select any data and only counts the results.
func (q *Query) SelectAndCount(
	ctx context.Context, values ...interface{},
) (count int, firstErr error) {
	if q.stickyErr != nil {
		return 0, q.stickyErr
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	if q.limit >= 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := q.Select(ctx, values...)
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
		count, err = q.Count(ctx)
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
func (q *Query) SelectAndCountEstimate(
	ctx context.Context, threshold int, values ...interface{},
) (count int, firstErr error) {
	if q.stickyErr != nil {
		return 0, q.stickyErr
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	if q.limit >= 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := q.Select(ctx, values...)
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
		count, err = q.CountEstimate(ctx, threshold)
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
//
// Function can accept a struct, a pointer to a struct, an orm.Model,
// or values for the columns in a row. Function must return an error.
func (q *Query) ForEach(ctx context.Context, fn interface{}) error {
	m := newFuncModel(fn)
	return q.Select(ctx, m)
}

func (q *Query) forEachHasOneJoin(fn func(*join) error) error {
	if q.tableModel == nil {
		return nil
	}
	return q._forEachHasOneJoin(fn, q.tableModel.GetJoins())
}

func (q *Query) _forEachHasOneJoin(fn func(*join) error, joins []join) error {
	for i := range joins {
		j := &joins[i]
		switch j.Rel.Type {
		case HasOneRelation, BelongsToRelation:
			err := fn(j)
			if err != nil {
				return err
			}

			err = q._forEachHasOneJoin(fn, j.JoinModel.GetJoins())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (q *Query) selectJoins(ctx context.Context, joins []join) error {
	var err error
	for i := range joins {
		j := &joins[i]
		if j.Rel.Type == HasOneRelation || j.Rel.Type == BelongsToRelation {
			err = q.selectJoins(ctx, j.JoinModel.GetJoins())
		} else {
			err = j.Select(ctx, q.db.Formatter(), q.New())
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Insert inserts the model.
func (q *Query) Insert(ctx context.Context, values ...interface{}) (Result, error) {
	if q.stickyErr != nil {
		return nil, q.stickyErr
	}

	model, err := q.newModel(values)
	if err != nil {
		return nil, err
	}

	if q.tableModel != nil && q.tableModel.Table().hasFlag(beforeInsertHookFlag) {
		ctx, err = q.tableModel.BeforeInsert(ctx)
		if err != nil {
			return nil, err
		}
	}

	query := NewInsertQuery(q)
	res, err := q.returningQuery(ctx, model, query)
	if err != nil {
		return nil, err
	}

	if q.tableModel != nil {
		if err := q.tableModel.AfterInsert(ctx); err != nil {
			return nil, err
		}
	}

	return res, nil
}

// SelectOrInsert selects the model inserting one if it does not exist.
// It returns true when model was inserted.
func (q *Query) SelectOrInsert(ctx context.Context, values ...interface{}) (inserted bool, _ error) {
	if q.stickyErr != nil {
		return false, q.stickyErr
	}

	var insertq *Query
	var insertErr error
	for i := 0; i < 5; i++ {
		if i >= 2 {
			dur := internal.RetryBackoff(i-2, 250*time.Millisecond, 5*time.Second)
			if err := internal.Sleep(ctx, dur); err != nil {
				return false, err
			}
		}

		err := q.Select(ctx, values...)
		if err == nil {
			return false, nil
		}
		if err != internal.ErrNoRows {
			return false, err
		}

		if insertq == nil {
			insertq = q
			if len(insertq.columns) > 0 {
				insertq = insertq.Clone()
				insertq.columns = nil
			}
		}

		res, err := insertq.Insert(ctx, values...)
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
		insertErr)
	return false, err
}

// Update updates the model.
func (q *Query) Update(ctx context.Context, scan ...interface{}) (Result, error) {
	return q.update(ctx, scan, false)
}

// Update updates the model omitting fields with zero values such as:
//   - empty string,
//   - 0,
//   - zero time,
//   - empty map or slice,
//   - byte array with all zeroes,
//   - nil ptr,
//   - types with method `IsZero() == true`.
func (q *Query) UpdateNotZero(ctx context.Context, scan ...interface{}) (Result, error) {
	return q.update(ctx, scan, true)
}

func (q *Query) update(ctx context.Context, values []interface{}, omitZero bool) (Result, error) {
	if q.stickyErr != nil {
		return nil, q.stickyErr
	}

	model, err := q.newModel(values)
	if err != nil {
		return nil, err
	}

	if q.tableModel != nil {
		ctx, err = q.tableModel.BeforeUpdate(ctx)
		if err != nil {
			return nil, err
		}
	}

	query := NewUpdateQuery(q, omitZero)
	res, err := q.returningQuery(ctx, model, query)
	if err != nil {
		return nil, err
	}

	if q.tableModel != nil {
		err = q.tableModel.AfterUpdate(ctx)
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (q *Query) returningQuery(ctx context.Context, model Model, query interface{}) (Result, error) {
	if !q.hasReturning() {
		return q.db.Query(ctx, model, query, q.tableModel)
	}
	if _, ok := model.(useQueryOne); ok {
		return q.db.QueryOne(ctx, model, query, q.tableModel)
	}
	return q.db.Query(ctx, model, query, q.tableModel)
}

// Delete deletes the model. When model has deleted_at column the row
// is soft deleted instead.
func (q *Query) Delete(ctx context.Context, values ...interface{}) (Result, error) {
	if q.tableModel == nil {
		return q.ForceDelete(ctx, values...)
	}

	table := q.tableModel.Table()
	if table.SoftDeleteField == nil {
		return q.ForceDelete(ctx, values...)
	}

	clone := q.Clone()
	if q.tableModel.IsNil() {
		if table.SoftDeleteField.SQLType == pgTypeBigint {
			clone = clone.Set("? = ?", table.SoftDeleteField.Column, time.Now().UnixNano())
		} else {
			clone = clone.Set("? = ?", table.SoftDeleteField.Column, time.Now())
		}
	} else {
		if err := clone.tableModel.setSoftDeleteField(); err != nil {
			return nil, err
		}
		clone = clone.Column(table.SoftDeleteField.SQLName)
	}
	return clone.Update(ctx, values...)
}

// Delete forces delete of the model with deleted_at column.
func (q *Query) ForceDelete(ctx context.Context, values ...interface{}) (Result, error) {
	if q.stickyErr != nil {
		return nil, q.stickyErr
	}
	q = q.withFlag(deletedFlag)

	model, err := q.newModel(values)
	if err != nil {
		return nil, err
	}

	if q.tableModel != nil {
		ctx, err = q.tableModel.BeforeDelete(ctx)
		if err != nil {
			return nil, err
		}
	}

	res, err := q.returningQuery(ctx, model, NewDeleteQuery(q))
	if err != nil {
		return nil, err
	}

	if q.tableModel != nil {
		if err := q.tableModel.AfterDelete(ctx); err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (q *Query) CreateTable(ctx context.Context, opt *CreateTableOptions) error {
	_, err := q.db.Exec(ctx, NewCreateTableQuery(q, opt))
	return err
}

func (q *Query) DropTable(ctx context.Context, opt *DropTableOptions) error {
	_, err := q.db.Exec(ctx, NewDropTableQuery(q, opt))
	return err
}

func (q *Query) CreateComposite(ctx context.Context, opt *CreateCompositeOptions) error {
	_, err := q.db.Exec(ctx, NewCreateCompositeQuery(q, opt))
	return err
}

func (q *Query) DropComposite(ctx context.Context, opt *DropCompositeOptions) error {
	_, err := q.db.Exec(ctx, NewDropCompositeQuery(q, opt))
	return err
}

// Exec is an alias for DB.Exec.
func (q *Query) Exec(
	ctx context.Context, query interface{}, params ...interface{},
) (Result, error) {
	params = append(params, q.tableModel)
	return q.db.Exec(ctx, query, params...)
}

// ExecOne is an alias for DB.ExecOne.
func (q *Query) ExecOne(
	ctx context.Context, query interface{}, params ...interface{},
) (Result, error) {
	params = append(params, q.tableModel)
	return q.db.ExecOne(ctx, query, params...)
}

// Query is an alias for DB.Query.
func (q *Query) Query(
	ctx context.Context, model, query interface{}, params ...interface{},
) (Result, error) {
	params = append(params, q.tableModel)
	return q.db.Query(ctx, model, query, params...)
}

// QueryOne is an alias for DB.QueryOne.
func (q *Query) QueryOne(
	ctx context.Context, model, query interface{}, params ...interface{},
) (Result, error) {
	params = append(params, q.tableModel)
	return q.db.QueryOne(ctx, model, query, params...)
}

// CopyFrom is an alias from DB.CopyFrom.
func (q *Query) CopyFrom(
	ctx context.Context, r io.Reader, query interface{}, params ...interface{},
) (Result, error) {
	params = append(params, q.tableModel)
	return q.db.CopyFrom(ctx, r, query, params...)
}

// CopyTo is an alias from DB.CopyTo.
func (q *Query) CopyTo(
	ctx context.Context, w io.Writer, query interface{}, params ...interface{},
) (Result, error) {
	params = append(params, q.tableModel)
	return q.db.CopyTo(ctx, w, query, params...)
}

var _ QueryAppender = (*Query)(nil)

func (q *Query) AppendQuery(fmter QueryFormatter, b []byte) ([]byte, error) {
	return NewSelectQuery(q).AppendQuery(fmter, b)
}

// Exists returns true or false depending if there are any rows matching the query.
func (q *Query) Exists(ctx context.Context) (bool, error) {
	q = q.Clone() // copy to not change original query
	q.columns = []QueryAppender{SafeQuery("1")}
	q.order = nil
	q.limit = 1
	res, err := q.db.Exec(ctx, NewSelectQuery(q))
	if err != nil {
		return false, err
	}
	return res.RowsAffected() > 0, nil
}

func (q *Query) hasTableModel() bool {
	return q.tableModel != nil && !q.tableModel.IsNil()
}

func (q *Query) hasExplicitTableModel() bool {
	return q.tableModel != nil && !q.hasFlag(implicitModelFlag)
}

func (q *Query) modelHasTableName() bool {
	return q.modelTableExpr != nil ||
		q.hasExplicitTableModel() && q.tableModel.Table().SQLName != ""
}

func (q *Query) hasTables() bool {
	return q.modelHasTableName() || len(q.tables) > 0
}

func (q *Query) appendFirstTable(fmter QueryFormatter, b []byte) ([]byte, error) {
	return q._appendFirstTable(fmter, b, false)
}

func (q *Query) appendFirstTableWithAlias(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	return q._appendFirstTable(fmter, b, true)
}

func (q *Query) _appendFirstTable(fmter QueryFormatter, b []byte, withAlias bool) ([]byte, error) {
	if q.modelTableExpr != nil {
		return q.modelTableExpr.AppendQuery(fmter, b)
	}
	if q.modelHasTableName() {
		table := q.tableModel.Table()
		b = fmter.FormatQuery(b, string(table.SQLName))
		if withAlias && table.Alias != table.SQLName {
			b = append(b, " AS "...)
			b = append(b, table.Alias...)
		}
		return b, nil
	}
	if len(q.tables) > 0 {
		return q.tables[0].AppendQuery(fmter, b)
	}
	return b, nil
}

func (q *Query) hasMultiTables() bool {
	if q.modelHasTableName() {
		return len(q.tables) >= 1
	}
	return len(q.tables) >= 2
}

func (q *Query) appendOtherTables(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	tables := q.tables
	if !q.modelHasTableName() {
		tables = tables[1:]
	}
	for i, f := range tables {
		if i > 0 {
			b = append(b, ", "...)
		}
		b, err = f.AppendQuery(fmter, b)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

func (q *Query) appendColumns(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	for i, f := range q.columns {
		if i > 0 {
			b = append(b, ", "...)
		}
		b, err = f.AppendQuery(fmter, b)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

func (q *Query) mustAppendSliceValues(
	fmter QueryFormatter, b []byte, ordering bool,
) (_ []byte, err error) {
	b = append(b, "("...)

	vq := ValuesQuery{
		q:        q,
		ordering: ordering,
	}
	fields := q.tableModel.Table().Fields
	if err != nil {
		return nil, err
	}

	b, err = vq.appendQuery(fmter, b, fields)
	if err != nil {
		return nil, err
	}

	b = append(b, ") AS _data("...)
	b = appendColumns(b, "", fields)
	if ordering {
		b = append(b, `, _ordering`...)
	}
	b = append(b, ')')

	return b, nil
}

func (q *Query) mustAppendWhere(fmter QueryFormatter, b []byte) ([]byte, error) {
	if len(q.where) == 0 && !q.hasFlag(wherePKFlag) {
		err := errors.New(
			"pg: Update and Delete queries require Where clause (try WherePK)")
		return nil, err
	}
	return q.appendWhere(fmter, b)
}

func (q *Query) appendWhere(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	if len(q.where) == 0 && !q.isSoftDelete() && !q.hasFlag(wherePKFlag) {
		return b, nil
	}

	b = append(b, " WHERE "...)
	startLen := len(b)

	if len(q.where) > 0 {
		b, err = q._appendWhere(fmter, b, q.where)
		if err != nil {
			return nil, err
		}
	}

	if q.isSoftDelete() {
		if len(b) > startLen {
			b = append(b, " AND "...)
		}
		b = append(b, q.tableModel.Table().Alias...)
		b = q.appendWhereSoftDelete(b)
	}

	if q.hasFlag(wherePKFlag) {
		if len(b) > startLen {
			b = append(b, " AND "...)
		}
		b, err = q.appendWherePK(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

func (q *Query) appendWhereSoftDelete(b []byte) []byte {
	b = append(b, '.')
	b = append(b, q.tableModel.Table().SoftDeleteField.Column...)
	if q.hasFlag(deletedFlag) {
		b = append(b, " IS NOT NULL"...)
	} else {
		b = append(b, " IS NULL"...)
	}
	return b
}

func (q *Query) appendWherePK(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	table := q.tableModel.Table()
	if err := table.checkPKs(); err != nil {
		return nil, err
	}

	if q.tableModel.Kind() == reflect.Struct {
		value := q.tableModel.Value()
		isPlaceholder := isTemplateFormatter(fmter)
		for i, f := range table.PKs {
			if i > 0 {
				b = append(b, " AND "...)
			}
			b = append(b, table.Alias...)
			b = append(b, '.')
			b = append(b, f.Column...)
			b = append(b, " = "...)
			if isPlaceholder {
				b = append(b, '?')
			} else {
				b = f.AppendValue(b, value, 1)
			}
		}
		return b, nil
	}

	for i, f := range table.PKs {
		if i > 0 {
			b = append(b, " AND "...)
		}
		b = append(b, table.Alias...)
		b = append(b, '.')
		b = append(b, f.Column...)
		b = append(b, " = "...)
		b = append(b, `"_data".`...)
		b = append(b, f.Column...)
	}
	return b, nil
}

func (q *Query) appendUpdWhere(fmter QueryFormatter, b []byte) ([]byte, error) {
	return q._appendWhere(fmter, b, q.updWhere)
}

func (q *Query) _appendWhere(
	fmter QueryFormatter, b []byte, where []queryWithSepAppender,
) (_ []byte, err error) {
	for i, f := range where {
		start := len(b)

		if i > 0 {
			b = f.AppendSep(b)
		}

		before := len(b)

		b, err = f.AppendQuery(fmter, b)
		if err != nil {
			return nil, err
		}

		if len(b) == before {
			b = b[:start]
		}
	}
	return b, nil
}

func (q *Query) appendSet(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	b = append(b, " SET "...)
	for i, f := range q.set {
		if i > 0 {
			b = append(b, ", "...)
		}
		b, err = f.AppendQuery(fmter, b)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

func (q *Query) hasReturning() bool {
	if len(q.returning) == 0 {
		return false
	}
	if len(q.returning) == 1 {
		switch q.returning[0].query {
		case "null", "NULL":
			return false
		}
	}
	return true
}

func (q *Query) appendReturning(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	if !q.hasReturning() {
		return b, nil
	}

	b = append(b, " RETURNING "...)
	for i, f := range q.returning {
		if i > 0 {
			b = append(b, ", "...)
		}
		b, err = f.AppendQuery(fmter, b)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

func (q *Query) appendWith(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	b = append(b, "WITH "...)
	for i, with := range q.with {
		if i > 0 {
			b = append(b, ", "...)
		}

		b = types.AppendIdent(b, with.name, 1)
		if q, ok := with.query.(ColumnsAppender); ok {
			b = append(b, " ("...)
			b, err = q.AppendColumns(fmter, b)
			if err != nil {
				return nil, err
			}
			b = append(b, ")"...)
		}

		b = append(b, " AS ("...)

		b, err = with.query.AppendQuery(fmter, b)
		if err != nil {
			return nil, err
		}

		b = append(b, ')')
	}
	b = append(b, ' ')
	return b, nil
}
