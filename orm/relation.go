package orm

import "gopkg.in/pg.v4/types"

const (
	HasOneRelation = 1 << iota
	BelongsToRelation
	HasManyRelation
	Many2ManyRelation
)

type Relation struct {
	Type        int
	Polymorphic bool
	Field       *Field
	Join        *Table
	FKs         []*Field

	M2MTableName types.Q
	BasePrefix   string
	JoinPrefix   string
}
