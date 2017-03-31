package orm

import "github.com/go-pg/pg/types"

type join struct {
	Parent     *join
	BaseModel  tableModel
	JoinModel  tableModel
	Rel        *Relation
	ApplyQuery func(*Query) (*Query, error)

	Columns []string
}

func (j *join) Select(db DB) error {
	switch j.Rel.Type {
	case HasManyRelation:
		return j.selectMany(db)
	case Many2ManyRelation:
		return j.selectM2M(db)
	}
	panic("not reached")
}

func (j *join) selectMany(db DB) error {
	q, err := j.manyQuery(db)
	if err != nil {
		return err
	}

	return q.Select()
}

func (j *join) manyQuery(db DB) (*Query, error) {
	root := j.JoinModel.Root()
	index := j.JoinModel.ParentIndex()

	manyModel := newManyModel(j)
	q := NewQuery(db, manyModel)
	if j.ApplyQuery != nil {
		var err error
		q, err = j.ApplyQuery(q)
		if err != nil {
			return nil, err
		}
	}

	q.columns = append(q.columns, hasManyColumnsAppender{j})

	baseTable := j.BaseModel.Table()
	cols := columns(j.JoinModel.Table().Alias, "", j.Rel.FKs)
	vals := values(root, index, baseTable.PKs)
	q = q.Where(`(?) IN (?)`, types.Q(cols), types.Q(vals))

	if j.Rel.Polymorphic {
		q = q.Where(
			`? IN (?, ?)`,
			types.F(j.Rel.BasePrefix+"type"),
			baseTable.ModelName, baseTable.TypeName,
		)
	}

	return q, nil
}

func (j *join) selectM2M(db DB) error {
	q, err := j.m2mQuery(db)
	if err != nil {
		return err
	}

	return q.Select()
}

func (j *join) m2mQuery(db DB) (*Query, error) {
	index := j.JoinModel.ParentIndex()

	baseTable := j.BaseModel.Table()
	m2mCols := columns(j.Rel.M2MTableName, j.Rel.BasePrefix, baseTable.PKs)
	m2mVals := values(j.BaseModel.Root(), index, baseTable.PKs)

	m2mModel := newM2MModel(j)
	q := NewQuery(db, m2mModel)
	if j.ApplyQuery != nil {
		var err error
		q, err = j.ApplyQuery(q)
		if err != nil {
			return nil, err
		}
	}

	q.columns = append(q.columns, hasManyColumnsAppender{j})
	q = q.Join(
		"JOIN ? ON (?) IN (?)",
		j.Rel.M2MTableName,
		types.Q(m2mCols), types.Q(m2mVals),
	)

	joinAlias := j.JoinModel.Table().Alias
	for _, pk := range j.JoinModel.Table().PKs {
		q = q.Where(
			"?.? = ?.?",
			joinAlias, pk.ColName,
			j.Rel.M2MTableName, types.F(j.Rel.JoinPrefix+pk.SQLName),
		)
	}

	return q, nil
}

func (j *join) hasParent() bool {
	if j.Parent != nil {
		switch j.Parent.Rel.Type {
		case HasOneRelation, BelongsToRelation:
			return true
		}
	}
	return false
}

func (j *join) appendAlias(b []byte) []byte {
	b = append(b, '"')
	b = appendAlias(b, j, true)
	b = append(b, '"')
	return b
}

func (j *join) appendAliasColumn(b []byte, column string) []byte {
	b = append(b, '"')
	b = appendAlias(b, j, true)
	b = append(b, "__"...)
	b = types.AppendField(b, column, 2)
	b = append(b, '"')
	return b
}

func (j *join) appendBaseAlias(b []byte) []byte {
	if j.hasParent() {
		b = append(b, '"')
		b = appendAlias(b, j.Parent, true)
		b = append(b, '"')
		return b
	}
	return append(b, j.BaseModel.Table().Alias...)
}

func appendAlias(b []byte, j *join, topLevel bool) []byte {
	if j.hasParent() {
		b = appendAlias(b, j.Parent, topLevel)
		topLevel = false
	}
	if !topLevel {
		b = append(b, "__"...)
	}
	b = append(b, j.Rel.Field.SQLName...)
	return b
}

func (j *join) appendHasOneColumns(b []byte) []byte {
	if j.Columns == nil {
		for i, f := range j.JoinModel.Table().Fields {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = j.appendAlias(b)
			b = append(b, '.')
			b = append(b, f.ColName...)
			b = append(b, " AS "...)
			b = j.appendAliasColumn(b, f.SQLName)
		}
		return b
	}

	for i, column := range j.Columns {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = j.appendAlias(b)
		b = append(b, '.')
		b = types.AppendField(b, column, 1)
		b = append(b, " AS "...)
		b = j.appendAliasColumn(b, column)
	}

	return b
}

func (j *join) appendHasOneJoin(b []byte) []byte {
	b = append(b, "LEFT JOIN "...)
	b = append(b, j.JoinModel.Table().Name...)
	b = append(b, " AS "...)
	b = j.appendAlias(b)

	b = append(b, " ON "...)
	if j.Rel.Type == HasOneRelation {
		joinTable := j.Rel.JoinTable
		for i, fk := range j.Rel.FKs {
			if i > 0 {
				b = append(b, " AND "...)
			}
			b = j.appendAlias(b)
			b = append(b, '.')
			b = append(b, joinTable.PKs[i].ColName...)
			b = append(b, " = "...)
			b = j.appendBaseAlias(b)
			b = append(b, '.')
			b = append(b, fk.ColName...)
		}
	} else {
		baseTable := j.BaseModel.Table()
		for i, fk := range j.Rel.FKs {
			if i > 0 {
				b = append(b, " AND "...)
			}
			b = j.appendAlias(b)
			b = append(b, '.')
			b = append(b, fk.ColName...)
			b = append(b, " = "...)
			b = j.appendBaseAlias(b)
			b = append(b, '.')
			b = append(b, baseTable.PKs[i].ColName...)
		}
	}

	return b
}

type hasManyColumnsAppender struct {
	*join
}

func (q hasManyColumnsAppender) AppendFormat(b []byte, f QueryFormatter) []byte {
	if q.Rel.M2MTableName != "" {
		b = append(b, q.Rel.M2MTableName...)
		b = append(b, ".*, "...)
	}

	joinTable := q.JoinModel.Table()

	if q.Columns == nil {
		for i, f := range joinTable.Fields {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = append(b, joinTable.Alias...)
			b = append(b, '.')
			b = append(b, f.ColName...)
		}
		return b
	}

	for i, column := range q.Columns {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = append(b, joinTable.Alias...)
		b = append(b, '.')
		b = types.AppendField(b, column, 1)
	}

	return b
}
