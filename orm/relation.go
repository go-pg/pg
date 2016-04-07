package orm

import "gopkg.in/pg.v4/types"

type Relation struct {
	One         bool
	Polymorphic string

	Field *Field
	Join  *Table

	FKs []*Field

	M2MTableName types.Q
}
