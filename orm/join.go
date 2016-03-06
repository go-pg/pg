package orm

import (
	"reflect"

	"gopkg.in/pg.v4/types"
)

type Join struct {
	BaseModel *TableModel
	JoinModel *TableModel
	Rel       *Relation

	SelectAll bool
	Columns   []string
}

func (j *Join) AppendColumns(columns []types.ValueAppender) []types.ValueAppender {
	alias := j.Rel.Field.SQLName
	prefix := alias + "__"

	if j.SelectAll {
		for _, f := range j.JoinModel.Table.Fields {
			q := Q("?.? AS ?", types.F(alias), types.F(f.SQLName), types.F(prefix+f.SQLName))
			columns = append(columns, q)
		}
	} else {
		for _, column := range j.Columns {
			q := Q("?.? AS ?", types.F(alias), types.F(column), types.F(prefix+column))
			columns = append(columns, q)
		}
	}

	return columns
}

func (j *Join) JoinOne(q *Query) {
	q.Table(j.JoinModel.Table.Name + " AS " + j.Rel.Field.SQLName)
	q.columns = j.AppendColumns(q.columns)
	for _, pk := range j.Rel.Join.PKs {
		q.Where(
			`?.? = ?.?`,
			types.F(j.Rel.Field.SQLName),
			types.F(pk.SQLName),
			types.F(j.BaseModel.Table.ModelName),
			types.F(j.Rel.Field.SQLName+"_"+pk.SQLName),
		)
	}
}

func (j *Join) Select(db dber, bind reflect.Value) error {
	if j.Rel.One {
		panic("not reached")
	} else if j.Rel.M2M != nil {
		return j.selectM2M(db, bind)
	} else {
		return j.selectMany(db, bind)
	}
}

func (j *Join) selectMany(db dber, bind reflect.Value) error {
	path := j.JoinModel.Path[:len(j.JoinModel.Path)-1]

	joinSlice := reflect.New(reflect.SliceOf(j.JoinModel.Table.Type)).Elem()
	j.JoinModel.slice = joinSlice

	q := NewQuery(db, j.JoinModel)
	q.columns = append(q.columns, types.F(j.JoinModel.Table.ModelName+".*"))

	cols := columns(j.JoinModel.Table.ModelName+".", j.Rel.FKs)
	vals := values(bind, path, j.BaseModel.Table.PKs...)
	q.Where(`(?) IN (?)`, types.Q(cols), types.Q(vals))

	if j.Rel.Polymorphic != "" {
		q.Where(`? = ?`, types.F(j.Rel.Polymorphic+"type"), j.BaseModel.Table.ModelName)
	}

	err := q.Select()
	if err != nil {
		return err
	}

	j.JoinModel.slice = invalidValue
	j.assignValues(bind, path, joinSlice)

	return nil
}

func (j *Join) assignValues(base reflect.Value, path []string, joinSlice reflect.Value) {
	if base.Kind() == reflect.Slice {
		j.assignValuesSlice(base, path, joinSlice)
	} else {
		j.assignValuesStruct(base, path, joinSlice)
	}
}

func (j *Join) assignValuesSlice(baseSlice reflect.Value, path []string, joinSlice reflect.Value) {
	for i := 0; i < baseSlice.Len(); i++ {
		j.assignValuesStruct(baseSlice.Index(i), path, joinSlice)
	}
}

func (j *Join) assignValuesStruct(baseStruct reflect.Value, path []string, joinSlice reflect.Value) {
	if len(path) > 0 {
		v := baseStruct.FieldByName(path[0])
		v = indirectNew(v)
		j.assignValues(v, path[1:], joinSlice)
		return
	}

	dst := j.Rel.Field.Value(baseStruct)
	dst = indirectNew(dst)

	if dst.Kind() == reflect.Slice {
		j.assignBelongsTo(dst, baseStruct, joinSlice)
	} else {
		j.assignHasOne(dst, baseStruct, joinSlice)
	}
}

func (j *Join) assignBelongsTo(dstSlice, baseStruct, joinSlice reflect.Value) {
	basePKs := make([]reflect.Value, len(j.BaseModel.Table.PKs))
	for i, pk := range j.BaseModel.Table.PKs {
		basePKs[i] = pk.Value(baseStruct)
	}

	for i := 0; i < joinSlice.Len(); i++ {
		joinStruct := joinSlice.Index(i)
		if j.belongsToEqual(basePKs, joinStruct) {
			dstSlice.Set(reflect.Append(dstSlice, joinStruct))
		}
	}
}

func (j *Join) belongsToEqual(basePKs []reflect.Value, joinStruct reflect.Value) bool {
	return equal(basePKs, joinStruct, j.Rel.FKs)
}

func (j *Join) assignHasOne(dstStruct, baseStruct, joinSlice reflect.Value) {
	baseFKs := make([]reflect.Value, len(j.Rel.FKs))
	for i, fk := range j.Rel.FKs {
		baseFKs[i] = fk.Value(baseStruct)
	}

	for i := 0; i < joinSlice.Len(); i++ {
		joinStruct := joinSlice.Index(i)
		if j.hasOneEqual(baseFKs, joinStruct) {
			dstStruct.Set(joinStruct)
		}
	}
}

func (j *Join) hasOneEqual(baseFKs []reflect.Value, joinStruct reflect.Value) bool {
	return equal(baseFKs, joinStruct, j.Rel.Join.PKs)
}

func (j *Join) selectM2M(db dber, bind reflect.Value) error {
	path := j.JoinModel.Path[:len(j.JoinModel.Path)-1]

	cols := columns("", j.Rel.M2MBaseFKs)
	vals := values(bind, path, j.BaseModel.Table.PKs...)

	m2mSlice := reflect.New(reflect.SliceOf(j.Rel.M2M.Type))
	q := NewQuery(db, m2mSlice)
	q.Where("(?) IN (?)", types.Q(cols), types.Q(vals))
	err := q.Select()
	if err != nil {
		return err
	}

	m2mSlice = m2mSlice.Elem()
	// TODO: reuse byte slices
	cols = columns("", j.JoinModel.Table.PKs)
	vals = values(m2mSlice, nil, j.Rel.M2MJoinFKs...)

	joinSlice := reflect.New(reflect.SliceOf(j.JoinModel.Table.Type)).Elem()
	j.JoinModel.slice = joinSlice
	q = NewQuery(db, j.JoinModel)
	q.Where(`(?) IN (?)`, types.Q(cols), types.Q(vals))

	err = q.Select()
	if err != nil {
		return err
	}

	j.JoinModel.slice = invalidValue
	j.assignM2MValues(bind, path, m2mSlice, joinSlice)

	return nil
}

func (j *Join) assignM2MValues(
	base reflect.Value, path []string, m2mSlice, joinSlice reflect.Value,
) {
	base = reflect.Indirect(base)
	if base.Kind() == reflect.Slice {
		j.assignM2MValuesSlice(base, path, m2mSlice, joinSlice)
	} else {
		j.assignM2MValuesStruct(base, path, m2mSlice, joinSlice)
	}
}

func (j *Join) assignM2MValuesSlice(
	baseSlice reflect.Value, path []string, m2mSlice, joinSlice reflect.Value,
) {
	for i := 0; i < baseSlice.Len(); i++ {
		j.assignM2MValuesStruct(baseSlice.Index(i), path, m2mSlice, joinSlice)
	}
}

func (j *Join) assignM2MValuesStruct(
	baseStruct reflect.Value, path []string, m2mSlice, joinSlice reflect.Value,
) {
	if len(path) > 0 {
		v := baseStruct.FieldByName(path[0])
		j.assignM2MValues(v, path[1:], m2mSlice, joinSlice)
		return
	}

	baseVals := make([]reflect.Value, len(j.BaseModel.Table.PKs))
	for i, pk := range j.BaseModel.Table.PKs {
		baseVals[i] = pk.Value(baseStruct)
	}

	baseSlice := j.Rel.Field.Value(baseStruct)
	for i := 0; i < m2mSlice.Len(); i++ {
		m2mStruct := m2mSlice.Index(i)
		if equal(baseVals, m2mStruct, j.Rel.M2MBaseFKs) {
			j.appendJoinedSlice(baseSlice, joinSlice, m2mStruct)
		}
	}
}

func (j *Join) appendJoinedSlice(baseSlice, joinSlice reflect.Value, m2mStruct reflect.Value) {
	m2mVals := make([]reflect.Value, len(j.Rel.M2MJoinFKs))
	for i, fk := range j.Rel.M2MJoinFKs {
		m2mVals[i] = fk.Value(m2mStruct)
	}

	for i := 0; i < joinSlice.Len(); i++ {
		joinStruct := joinSlice.Index(i)

		if equal(m2mVals, joinStruct, j.JoinModel.Table.PKs) {
			copyFields(
				joinStruct,
				m2mStruct,
				j.JoinModel.Table.FieldsMap,
				j.Rel.M2MModelFields(j.JoinModel.Table.ModelName),
			)
			baseSlice.Set(reflect.Append(baseSlice, joinStruct))
			break
		}
	}
}

func copyFields(dst, m2m reflect.Value, dstFields map[string]*Field, modelFields m2mModelFields) {
	for _, f := range modelFields.Fields {
		dstFieldName := f.SQLName[len(modelFields.Prefix):]
		dstField := dstFields[dstFieldName]
		dstField.Value(dst).Set(f.Value(m2m))
	}
}
