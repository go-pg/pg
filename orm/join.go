package orm

import "gopkg.in/pg.v4/types"

type Join struct {
	BaseModel TableModel
	JoinModel TableModel
	Rel       *Relation

	SelectAll bool
	Columns   []string
}

func (j *Join) AppendColumns(dst []byte) []byte {
	alias := make([]byte, 0, 3*len(j.Rel.Field.SQLName))
	alias = append(alias, j.Rel.Field.SQLName...)
	alias = append(alias, "__"...)

	if j.SelectAll {
		for _, f := range j.JoinModel.Table().Fields {
			dst = appendSep(dst, ", ")
			dst = append(dst, j.Rel.Field.ColName...)
			dst = append(dst, '.')
			dst, _ = f.ColName.AppendValue(dst, 1)
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

func (j *Join) JoinOne(q *Query) {
	var cond types.Q
	for i, pk := range j.Rel.Join.PKs {
		cond = q.format(
			cond,
			`?.? = ?.?`,
			j.Rel.Field.ColName,
			pk.ColName,
			types.Q(j.BaseModel.Table().ModelName),
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

func (j *Join) Select(db dber) error {
	if j.Rel.One {
		panic("not reached")
	} else if len(j.Rel.M2MTableName) > 0 {
		return j.selectM2M(db)
	} else {
		return j.selectMany(db)
	}
}

func (j *Join) selectMany(db dber) error {
	root := j.JoinModel.Root()
	path := j.JoinModel.Path()
	path = path[:len(path)-1]

	manyModel := newManyModel(j)
	q := NewQuery(db, manyModel)

	q.columns = appendSep(q.columns, ", ")
	q.columns = types.AppendField(q.columns, j.JoinModel.Table().ModelName, 1)
	q.columns = append(q.columns, ".*"...)

	cols := columns(col(j.JoinModel.Table().ModelName), "", j.Rel.FKs)
	vals := values(root, path, j.BaseModel.Table().PKs)
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

func (j *Join) selectM2M(db dber) error {
	path := j.JoinModel.Path()
	path = path[:len(path)-1]

	baseTable := j.BaseModel.Table()
	m2mCols := columns(j.Rel.M2MTableName, j.Rel.BasePrefix, baseTable.PKs)
	m2mVals := values(j.BaseModel.Root(), path, baseTable.PKs)

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
			types.F(joinTable.ModelName), pk.ColName,
			j.Rel.M2MTableName, types.F(j.Rel.JoinPrefix+pk.SQLName),
		)
	}

	err := q.Select()
	if err != nil {
		return err
	}

	return nil
}
