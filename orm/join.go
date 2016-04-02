package orm

import "gopkg.in/pg.v4/types"

type Join struct {
	BaseModel TableModel
	JoinModel TableModel
	Rel       *Relation

	SelectAll bool
	Columns   []string
}

func (j *Join) AppendColumns(columns []byte) []byte {
	alias := j.Rel.Field.SQLName
	prefix := alias + "__"

	if j.SelectAll {
		for _, f := range j.JoinModel.Table().Fields {
			q := Q("?.? AS ?", types.F(alias), types.F(f.SQLName), types.F(prefix+f.SQLName))

			columns = appendSep(columns, ", ")
			columns = append(columns, q...)
		}
	} else {
		for _, column := range j.Columns {
			q := Q("?.? AS ?", types.F(alias), types.F(column), types.F(prefix+column))

			columns = appendSep(columns, ", ")
			columns = append(columns, q...)
		}
	}

	return columns
}

func (j *Join) JoinOne(q *Query) {
	var cond types.Q
	for i, pk := range j.Rel.Join.PKs {
		cond = Formatter{}.Append(
			cond,
			`?.? = ?.?`,
			types.F(j.Rel.Field.SQLName),
			types.F(pk.SQLName),
			types.F(j.BaseModel.Table().ModelName),
			types.F(j.Rel.Field.SQLName+"_"+pk.SQLName),
		)
		if i != len(j.Rel.Join.PKs)-1 {
			cond = append(cond, " AND "...)
		}
	}

	q.Join(
		"LEFT JOIN ? AS ? ON ?",
		types.F(j.JoinModel.Table().Name), types.F(j.Rel.Field.SQLName), cond,
	)
	q.columns = j.AppendColumns(q.columns)
}

func (j *Join) Select(db dber) error {
	if j.Rel.One {
		panic("not reached")
	} else if j.Rel.M2MTableName != "" {
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
	q.columns = types.AppendField(q.columns, j.JoinModel.Table().ModelName+".*", 1)

	cols := columns(j.JoinModel.Table().ModelName+".", j.Rel.FKs)
	vals := values(root, path, j.BaseModel.Table().PKs)
	q.Where(`(?) IN (?)`, types.Q(cols), types.Q(vals))

	if j.Rel.Polymorphic != "" {
		q.Where(`? = ?`, types.F(j.Rel.Polymorphic+"type"), j.BaseModel.Table().ModelName)
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
	m2mCols := columns(j.Rel.M2MTableName+"."+baseTable.ModelName+"_", baseTable.PKs)
	m2mVals := values(j.BaseModel.Root(), path, baseTable.PKs)

	m2mModel := newM2MModel(j)
	q := NewQuery(db, m2mModel)
	q.Column("*")
	q.Join(
		"JOIN ? ON (?) IN (?)",
		types.F(j.Rel.M2MTableName),
		types.Q(m2mCols), types.Q(m2mVals),
	)
	joinTable := j.JoinModel.Table()
	for _, pk := range joinTable.PKs {
		q.Where(
			"?.? = ?.?",
			types.F(joinTable.ModelName), types.F(pk.SQLName),
			types.F(j.Rel.M2MTableName), types.F(joinTable.ModelName+"_"+pk.SQLName),
		)
	}

	err := q.Select()
	if err != nil {
		return err
	}

	return nil
}
