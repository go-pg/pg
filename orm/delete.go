package orm

import "fmt"

func Delete(db dber, v interface{}) error {
	q := NewQuery(db, v)
	if q.err != nil {
		return q.err
	}
	_, err := db.ExecOne(deleteModel{q}, q.model)
	return err
}

type deleteModel struct {
	*Query
}

var _ QueryAppender = (*deleteModel)(nil)

func (del deleteModel) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	b = append(b, "DELETE FROM "...)
	b = append(b, del.tableName...)

	b = append(b, " WHERE "...)
	if len(del.where) > 0 {
		b = append(b, del.where...)
	} else {
		table := del.model.Table()
		if len(table.PKs) == 0 {
			err := fmt.Errorf(
				"can't delete model %q without primary keys",
				table.ModelName,
			)
			return nil, err
		}
		b = appendFieldValue(b, del.model.Value(), table.PKs)
	}

	return b, nil
}
