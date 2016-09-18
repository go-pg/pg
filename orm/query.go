package orm

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"gopkg.in/pg.v4/internal"
	"gopkg.in/pg.v4/types"
)

type Query struct {
	db    DB
	model tableModel
	err   error

	tableAlias string

	with       []byte
	tables     []byte
	fields     []string
	columns    []byte
	rels       map[string]func(*Query) *Query
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

func NewQuery(db DB, model ...interface{}) *Query {
	return (&Query{}).DB(db).Model(model...)
}

func (q *Query) copy() *Query {
	cp := *q
	return &cp
}

// Err sets the err returned when query is executed.
func (q *Query) Err(err error) *Query {
	if q.err == nil {
		q.err = err
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
		q = q.Err(err)
	}
	return q
}

func (q *Query) With(name string, subq *Query) *Query {
	var err error
	q.with = appendSep(q.with, ", ")
	q.with = types.AppendField(q.with, name, 1)
	q.with = append(q.with, " AS ("...)
	q.with, err = selectQuery{subq}.AppendQuery(q.with)
	if err != nil {
		q = q.Err(err)
	}
	q.with = append(q.with, ')')
	return q
}

func (q *Query) Table(names ...string) *Query {
	for _, name := range names {
		q.tables = appendSep(q.tables, ", ")
		q.tables = types.AppendField(q.tables, name, 1)
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
		q.columns = appendSep(q.columns, ", ")
		q.columns = types.AppendField(q.columns, column, 1)
	}
	return q
}

func (q *Query) ColumnExpr(expr string, params ...interface{}) *Query {
	q.columns = appendSep(q.columns, ", ")
	q.columns = q.FormatQuery(q.columns, expr, params...)
	return q
}

func (q *Query) Relation(name string, apply func(*Query) *Query) *Query {
	if j := q.model.Join(name, apply); j == nil {
		q.err = fmt.Errorf(
			"model %s does not have relation %s",
			q.model.Table().Type.Name(), name,
		)
	}
	return q
}

func (q *Query) Set(set string, params ...interface{}) *Query {
	if q.onConflictDoUpdate() {
		return q.onConflictSet(set, params...)
	}

	q.set = appendSep(q.set, ", ")
	q.set = q.FormatQuery(q.set, set, params...)
	return q
}

func (q *Query) onConflictSet(set string, params ...interface{}) *Query {
	ind := bytes.LastIndex(q.onConflict, []byte(" DO UPDATE"))
	if ind == -1 {
		return q
	}
	if bytes.Contains(q.onConflict[ind:], []byte(" SET ")) {
		q.onConflict = append(q.onConflict, ", "...)
	} else {
		q.onConflict = append(q.onConflict, " SET "...)
	}
	q.onConflict = q.FormatQuery(q.onConflict, set, params...)
	return q
}

func (q *Query) Where(where string, params ...interface{}) *Query {
	if q.onConflictDoUpdate() {
		return q.onConflictWhere(where, params...)
	}

	q.where = appendSep(q.where, " AND ")
	q.where = append(q.where, '(')
	q.where = q.FormatQuery(q.where, where, params...)
	q.where = append(q.where, ')')
	return q
}

func (q *Query) onConflictWhere(where string, params ...interface{}) *Query {
	ind := bytes.LastIndex(q.onConflict, []byte(" DO UPDATE"))
	if ind == -1 {
		return q
	}
	if bytes.Contains(q.onConflict[ind:], []byte(" WHERE ")) {
		q.onConflict = append(q.onConflict, " AND "...)
	} else {
		q.onConflict = append(q.onConflict, " WHERE "...)
	}
	q.onConflict = append(q.onConflict, '(')
	q.onConflict = q.FormatQuery(q.onConflict, where, params...)
	q.onConflict = append(q.onConflict, ')')
	return q
}

// WhereOr joins passed conditions using OR operation.
func (q *Query) WhereOr(conditions ...*SQL) *Query {
	q.where = appendSep(q.where, " AND ")
	q.where = append(q.where, '(')
	for i, cond := range conditions {
		q.where = cond.AppendFormat(q.where, q)
		if i != len(conditions)-1 {
			q.where = append(q.where, " OR "...)
		}
	}
	q.where = append(q.where, ')')
	return q
}

func (q *Query) Join(join string, params ...interface{}) *Query {
	q.join = appendSep(q.join, " ")
	q.join = q.FormatQuery(q.join, join, params...)
	return q
}

func (q *Query) Group(group string, params ...interface{}) *Query {
	q.group = appendSep(q.group, ", ")
	q.group = q.FormatQuery(q.group, group, params...)
	return q
}

func (q *Query) Order(order string, params ...interface{}) *Query {
	q.order = appendSep(q.order, ", ")
	q.order = q.FormatQuery(q.order, order, params...)
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
	q.onConflict = append(q.onConflict, " ON CONFLICT "...)
	q.onConflict = q.FormatQuery(q.onConflict, s, params...)
	return q
}

func (q *Query) onConflictDoUpdate() bool {
	return len(q.onConflict) > 0 &&
		bytes.Contains(q.onConflict, []byte(" DO UPDATE"))
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
				q = q.Err(err)
			}
		default:
			q = q.Err(fmt.Errorf("unsupported column type: %T", column))
		}
	}
	return q
}

// Apply calls the fn passing the Query as an argument.
func (q *Query) Apply(fn func(*Query) *Query) *Query {
	return fn(q)
}

// Count returns number of rows matching the query using count aggregate function.
func (q *Query) Count() (int, error) {
	if q.err != nil {
		return 0, q.err
	}

	q = q.copy()
	q.columns = types.Q("count(*)")
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
	if q.err != nil {
		return q.err
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

// Create inserts the model.
func (q *Query) Create(values ...interface{}) (*types.Result, error) {
	if q.err != nil {
		return nil, q.err
	}

	var model Model
	if len(values) > 0 {
		model = Scan(values...)
	} else if q.model != nil {
		model = q.model
	}

	if q.model != nil {
		if err := q.model.BeforeCreate(q.db); err != nil {
			return nil, err
		}
	}

	ins := insertQuery{Query: q}
	res, err := q.db.Query(model, ins, q.model)
	if err != nil {
		return nil, err
	}

	if q.model != nil {
		if err := q.model.AfterCreate(q.db); err != nil {
			return nil, err
		}
	}

	return res, nil
}

// SelectOrCreate selects the model creating one if it does not exist.
func (q *Query) SelectOrCreate(values ...interface{}) (created bool, err error) {
	if q.err != nil {
		return false, q.err
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

		res, err := q.Create(values...)
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
		"pg: SelectOrCreate: select returns no rows (insert fails with err=%q)",
		insertErr,
	)
	return false, err
}

// Update updates the model.
func (q *Query) Update(values ...interface{}) (*types.Result, error) {
	if q.err != nil {
		return nil, q.err
	}

	model, err := q.newModel(values)
	if err != nil {
		return nil, err
	}

	return q.db.Query(model, updateQuery{q}, q.model)
}

// Delete deletes the model.
func (q *Query) Delete() (*types.Result, error) {
	if q.err != nil {
		return nil, q.err
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
	b = append(b, q.tables...)
	return b
}

func (q *Query) appendSet(b []byte) ([]byte, error) {
	b = append(b, " SET "...)
	if len(q.set) > 0 {
		b = append(b, q.set...)
		return b, nil
	}

	if q.model == nil {
		return nil, errors.New("pg: Model(nil)")
	}

	table := q.model.Table()
	strct := q.model.Value()

	if len(q.fields) > 0 {
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
		return b, nil
	}

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
	return b, nil
}

func (q *Query) appendWhere(b []byte) ([]byte, error) {
	b = append(b, " WHERE "...)
	if len(q.where) > 0 {
		b = append(b, q.where...)
		return b, nil
	}

	if q.model == nil {
		return nil, errors.New("pg: Model(nil)")
	}

	table := q.model.Table()
	if err := table.checkPKs(); err != nil {
		return nil, err
	}
	b = appendColumnAndValue(b, q.model.Value(), table, table.PKs)
	return b, nil
}
