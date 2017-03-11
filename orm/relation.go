package orm

import "github.com/go-pg/pg/types"

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
	JoinTable   *Table
	FKs         []*Field

	M2MTableName types.Q
	BasePrefix   string
	JoinPrefix   string
}
