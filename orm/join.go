package orm

import "gopkg.in/pg.v4/types"

type join struct {
	BaseModel tableModel
	JoinModel tableModel
	Rel       *Relation

	SelectAll bool
	Columns   []string
}

func (j *join) AppendColumns(dst []byte) []byte {
	alias := make([]byte, 0, 3*len(j.Rel.Field.SQLName))
	alias = append(alias, j.Rel.Field.SQLName...)
	alias = append(alias, "__"...)

	if j.SelectAll {
		for _, f := range j.JoinModel.Table().Fields {
			dst = appendSep(dst, ", ")
			dst = append(dst, j.Rel.Field.ColName...)
			dst = append(dst, '.')
			dst = append(dst, f.ColName...)
			dst = append(dst, " AS "...)
			dst = types.AppendFieldBytes(dst, append(alias, f.SQLName...), 1)
		}
	} else {
		for _, column := range j.Columns {
			dst = appendSep(dst, ", ")
			dst = append(dst, j.Rel.Field.ColName...)
			dst = append(dst, '.')
			dst = types.AppendField(dst, column, 1)
			dst = append(dst, " AS "...)
			dst = types.AppendFieldBytes(dst, append(alias, column...), 1)
		}
	}

	return dst
}

func (j *join) JoinOne(q *Query) {
	var cond types.Q
	for i, pk := range j.Rel.Join.PKs {
		cond = q.FormatQuery(
			cond,
			`?.? = ?.?`,
			j.Rel.Field.ColName,
			pk.ColName,
			j.BaseModel.Table().Alias,
			types.F(j.Rel.Field.SQLName+"_"+pk.SQLName),
		)
		if i != len(j.Rel.Join.PKs)-1 {
			cond = append(cond, " AND "...)
		}
	}

	q.Join(
		"LEFT JOIN ? AS ? ON ?",
		j.JoinModel.Table().Name, j.Rel.Field.ColName, cond,
	)
	q.columns = j.AppendColumns(q.columns)
}

func (j *join) Select(db dber) error {
	if j.Rel.One {
		panic("not reached")
	} else if len(j.Rel.M2MTableName) > 0 {
		return j.selectM2M(db)
	} else {
		return j.selectMany(db)
	}
}

func (j *join) selectMany(db dber) error {
	root := j.JoinModel.Root()
	index := j.JoinModel.Index()
	index = index[:len(index)-1]

	manyModel := newManyModel(j)
	q := NewQuery(db, manyModel)

	q.columns = appendSep(q.columns, ", ")
	q.columns = append(q.columns, j.JoinModel.Table().Alias...)
	q.columns = append(q.columns, ".*"...)

	cols := columns(j.JoinModel.Table().Alias, "", j.Rel.FKs)
	vals := values(root, index, j.BaseModel.Table().PKs)
	q.Where(`(?) IN (?)`, types.Q(cols), types.Q(vals))

	if j.Rel.Polymorphic {
		q.Where(`? = ?`, types.F(j.Rel.BasePrefix+"type"), j.BaseModel.Table().ModelName)
	}

	err := q.Select()
	if err != nil {
		return err
	}

	return nil
}

func (j *join) selectM2M(db dber) error {
	index := j.JoinModel.Index()
	index = index[:len(index)-1]

	baseTable := j.BaseModel.Table()
	m2mCols := columns(j.Rel.M2MTableName, j.Rel.BasePrefix, baseTable.PKs)
	m2mVals := values(j.BaseModel.Root(), index, baseTable.PKs)

	m2mModel := newM2MModel(j)
	q := NewQuery(db, m2mModel)
	q.Column("*")
	q.Join(
		"JOIN ? ON (?) IN (?)",
		j.Rel.M2MTableName,
		types.Q(m2mCols), types.Q(m2mVals),
	)
	joinTable := j.JoinModel.Table()
	for _, pk := range joinTable.PKs {
		q.Where(
			"?.? = ?.?",
			joinTable.Alias, pk.ColName,
			j.Rel.M2MTableName, types.F(j.Rel.JoinPrefix+pk.SQLName),
		)
	}

	err := q.Select()
	if err != nil {
		return err
	}

	return nil
}
