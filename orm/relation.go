package orm

import (
	"fmt"
	"strings"
)

type HasOne struct {
	Base, Join *Model
	Field      *Field
}

func (h *HasOne) Select(s *Select) *Select {
	s = s.Table(h.Join.Table.Name)
	s.columns = h.Join.Columns(s.columns, h.Join.Table.Name+"__")
	s = s.Where(h.where())
	return s
}

func (h *HasOne) where() string {
	return h.Join.Table.Name + ".id" + " = " + h.Base.Table.Name + "." + h.Field.SQLName + "_id"
}

type HasMany struct {
	Base, Join *Model
	Field      *Field
}

func (h *HasMany) Select(s *Select) *Select {
	s = s.Where(h.where())
	return s
}

func (h *HasMany) where() string {
	return h.Join.Table.Name + "." + h.Base.Table.Name + "_id" + " = " + h.Base.PKValue()
}

type Relation struct {
	Model   *Model
	HasOne  map[string]HasOne
	HasMany []HasMany
}

var (
	_ Collection    = (*Relation)(nil)
	_ ColumnScanner = (*Relation)(nil)
)

func NewRelation(vi interface{}) (*Relation, error) {
	switch v := vi.(type) {
	case *Relation:
		return v, nil
	case *Model:
		return &Relation{
			Model: v,
		}, nil
	default:
		model, err := NewModel(vi)
		if err != nil {
			return nil, err
		}
		return &Relation{
			Model: model,
		}, nil
	}
}

func (rel *Relation) AddRelation(name string) (err error) {
	path := strings.Split(name, ".")

	base := rel.Model
	value := base.Value(false)

	for _, name := range path {
		if field, ok := base.Table.HasOne[name]; ok {
			model, err := NewModelPath(value, []string{field.GoName})
			if err != nil {
				return err
			}
			if rel.HasOne == nil {
				rel.HasOne = make(map[string]HasOne)
			}
			rel.HasOne[name] = HasOne{
				Base:  base,
				Join:  model,
				Field: field,
			}
			continue
		}

		if field, ok := base.Table.HasMany[name]; ok {
			model, err := NewModelPath(value, []string{field.GoName})
			if err != nil {
				return err
			}
			rel.HasMany = append(rel.HasMany, HasMany{
				Base:  base,
				Join:  model,
				Field: field,
			})
			continue
		}

		return fmt.Errorf("pg: %s doesn't have %s relation", base.Table.Name, name)
	}

	return nil
}

func (rel *Relation) AppendParam(b []byte, name string) ([]byte, error) {
	return rel.Model.AppendParam(b, name)
}

func (rel *Relation) NextModel() interface{} {
	rel.Model.NextModel()
	// TODO: rebind has one relations
	return rel
}

func splitColumn(s string) (string, string) {
	parts := strings.SplitN(s, "__", 2)
	if len(parts) != 2 {
		return "", s
	}
	return parts[0], parts[1]
}

func (rel *Relation) ScanColumn(colIdx int, colName string, b []byte) error {
	modelName, colName := splitColumn(colName)
	hasOne, ok := rel.HasOne[modelName]
	if ok {
		return hasOne.Join.ScanColumn(colIdx, colName, b)
	}
	return rel.Model.ScanColumn(colIdx, colName, b)
}
