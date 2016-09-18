package orm

import "gopkg.in/pg.v4/types"

type hasOneJoinQuery struct {
	*join
}

func (q hasOneJoinQuery) AppendFormat(b []byte, f QueryFormatter) []byte {
	b = append(b, "LEFT JOIN "...)
	b = append(b, q.JoinModel.Table().Name...)
	b = append(b, " AS "...)
	b = append(b, q.Rel.Field.ColName...)

	joinTable := q.Rel.JoinTable
	b = append(b, " ON "...)
	for i, fk := range q.Rel.FKs {
		if i > 0 {
			b = append(b, " AND "...)
		}
		b = append(b, q.Rel.Field.ColName...)
		b = append(b, '.')
		b = append(b, joinTable.PKs[i].ColName...)
		b = append(b, " = "...)
		b = append(b, q.BaseModel.Table().Alias...)
		b = append(b, '.')
		b = append(b, fk.ColName...)
	}

	return b
}

type hasOneColumnsQuery struct {
	*join
}

func (q hasOneColumnsQuery) AppendFormat(b []byte, f QueryFormatter) []byte {
	alias := q.alias()

	if q.Columns == nil {
		for i, f := range q.JoinModel.Table().Fields {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = append(b, q.Rel.Field.ColName...)
			b = append(b, '.')
			b = append(b, f.ColName...)
			b = append(b, " AS "...)
			columnAlias := append(alias, f.SQLName...)
			b = types.AppendFieldBytes(b, columnAlias, 1)
			alias = columnAlias[:len(alias)]
		}
		return b
	}

	for i, column := range q.Columns {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = append(b, q.Rel.Field.ColName...)
		b = append(b, '.')
		b = types.AppendField(b, column, 1)
		b = append(b, " AS "...)
		columnAlias := append(alias, column...)
		b = types.AppendFieldBytes(b, append(alias, column...), 1)
		alias = columnAlias[:len(alias)]
	}

	return b
}

type belongsToJoinQuery struct {
	*join
}

func (q belongsToJoinQuery) AppendFormat(b []byte, f QueryFormatter) []byte {
	b = append(b, "LEFT JOIN "...)
	b = append(b, q.JoinModel.Table().Name...)
	b = append(b, " AS "...)
	b = append(b, q.Rel.Field.ColName...)

	baseTable := q.BaseModel.Table()
	b = append(b, " ON "...)
	for i, fk := range q.Rel.FKs {
		if i > 0 {
			b = append(b, " AND "...)
		}
		b = append(b, q.Rel.Field.ColName...)
		b = append(b, '.')
		b = append(b, fk.ColName...)
		b = append(b, " = "...)
		b = append(b, baseTable.Alias...)
		b = append(b, '.')
		b = append(b, baseTable.PKs[i].ColName...)
	}

	return b
}

type manyColumnsQuery struct {
	*join
}

func (q manyColumnsQuery) AppendFormat(b []byte, f QueryFormatter) []byte {
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
