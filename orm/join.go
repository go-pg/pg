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

func (j *join) JoinHasOne(q *Query) {
	if j.hasColumns() {
		q.columns = append(q.columns, hasOneQueryColumns{j})
	}
	q.joins = append(q.joins, hasOneQueryJoin{j})
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

	q.columns = append(q.columns, manyColumnsQuery{j})

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

	q.columns = append(q.columns, manyColumnsQuery{j})
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

func (q *join) hasColumns() bool {
	return len(q.Columns) != 0 || q.Columns == nil
}
