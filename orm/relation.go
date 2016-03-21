package orm

type Relation struct {
	One         bool
	Polymorphic string

	Field *Field
	Join  *Table

	FKs []*Field

	M2MTableName string
}
