package orm

import "gopkg.in/pg.v5/types"

type hasOneQueryJoin struct {
	*join
}

func (q hasOneQueryJoin) AppendFormat(b []byte, f QueryFormatter) []byte {
	b = append(b, "LEFT JOIN "...)
	b = append(b, q.JoinModel.Table().Name...)
	b = append(b, " AS "...)
	b = q.appendAlias(b)

	b = append(b, " ON "...)
	if q.Rel.Type == HasOneRelation {
		joinTable := q.Rel.JoinTable
		for i, fk := range q.Rel.FKs {
			if i > 0 {
				b = append(b, " AND "...)
			}
			b = q.appendAlias(b)
			b = append(b, '.')
			b = append(b, joinTable.PKs[i].ColName...)
			b = append(b, " = "...)
			b = q.appendBaseAlias(b)
			b = append(b, '.')
			b = append(b, fk.ColName...)
		}
	} else {
		baseTable := q.BaseModel.Table()
		for i, fk := range q.Rel.FKs {
			if i > 0 {
				b = append(b, " AND "...)
			}
			b = q.appendAlias(b)
			b = append(b, '.')
			b = append(b, fk.ColName...)
			b = append(b, " = "...)
			b = q.appendBaseAlias(b)
			b = append(b, '.')
			b = append(b, baseTable.PKs[i].ColName...)
		}
	}

	return b
}

type hasOneQueryColumns struct {
	*join
}

func (q hasOneQueryColumns) AppendFormat(b []byte, f QueryFormatter) []byte {
	alias := q.appendAlias(nil)
	prefix := append(alias, "__"...)

	if q.Columns == nil {
		for i, f := range q.JoinModel.Table().Fields {
			if i > 0 {
				b = append(b, ", "...)
			}
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

	for i, column := range q.Columns {
		if i > 0 {
			b = append(b, ", "...)
		}
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
