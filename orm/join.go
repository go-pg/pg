package orm

import "gopkg.in/pg.v5/types"

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

func (j *join) selectMany(db DB) (err error) {
	root := j.JoinModel.Root()
	index := j.JoinModel.ParentIndex()

	manyModel := newManyModel(j)
	q := NewQuery(db, manyModel)
	if j.ApplyQuery != nil {
		q, err = j.ApplyQuery(q)
		if err != nil {
			return err
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

	err = q.Select()
	if err != nil {
		return err
	}

	return nil
}

func (j *join) selectM2M(db DB) (err error) {
	index := j.JoinModel.ParentIndex()

	baseTable := j.BaseModel.Table()
	m2mCols := columns(j.Rel.M2MTableName, j.Rel.BasePrefix, baseTable.PKs)
	m2mVals := values(j.BaseModel.Root(), index, baseTable.PKs)

	m2mModel := newM2MModel(j)
	q := NewQuery(db, m2mModel)
	if j.ApplyQuery != nil {
		q, err = j.ApplyQuery(q)
		if err != nil {
			return err
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

	err = q.Select()
	if err != nil {
		return err
	}

	return nil
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
	return appendAlias(b, j, true)
}

func (j *join) appendBaseAlias(b []byte) []byte {
	if j.hasParent() {
		return appendAlias(b, j.Parent, true)
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
	alias := j.appendAlias(nil)
	prefix := append(alias, "__"...)

	if j.Columns == nil {
		for _, f := range j.JoinModel.Table().Fields {
			b = append(b, ", "...)
			b = append(b, alias...)
			b = append(b, '.')
			b = append(b, f.ColName...)
			b = append(b, " AS "...)
			columnAlias := append(prefix, f.SQLName...)
			b = types.AppendFieldBytes(b, columnAlias, 1)
			prefix = columnAlias[:len(prefix)]
		}
		return b
	}

	for _, column := range j.Columns {
		b = append(b, ", "...)
		b = append(b, alias...)
		b = append(b, '.')
		b = types.AppendField(b, column, 1)
		b = append(b, " AS "...)
		columnAlias := append(prefix, column...)
		b = types.AppendFieldBytes(b, columnAlias, 1)
		prefix = columnAlias[:len(prefix)]
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

	if q.Columns == nil {
		b = append(b, q.JoinModel.Table().Alias...)
		b = append(b, ".*"...)
		return b
	}

	for i, column := range q.Columns {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = append(b, q.JoinModel.Table().Alias...)
		b = append(b, '.')
		b = types.AppendField(b, column, 1)
	}

	return b
}
