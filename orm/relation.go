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
	Field       *Field
	JoinTable   *Table
	FKs         []*Field
	Polymorphic *Field
	FKValues    []*Field

	M2MTableName  types.Q
	M2MTableAlias types.Q
	BaseFKs       []string
	JoinFKs       []string
}
