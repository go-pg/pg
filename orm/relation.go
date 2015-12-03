package orm

import (
	"fmt"
	"strings"
)

type Relation struct {
	Model   *Model
	HasOne  map[string]*Model
	HasMany []*Model
}

func NewRelation(vi interface{}) (*Relation, error) {
	model, ok := vi.(*Model)
	if !ok {
		var err error
		model, err = NewModel(vi)
		if err != nil {
			return nil, err
		}

	}
	return &Relation{
		Model: model,
	}, nil
}

func (rel *Relation) AddRelation(name string) (err error) {
	path := strings.Split(name, ".")

	model := rel.Model
	value := model.Value(false)

	for i, name := range path {
		if _, ok := model.Table.HasOne[name]; ok {
			model, err = NewModelPath(value, path[:i+1])
			if err != nil {
				return err
			}
			if rel.HasOne == nil {
				rel.HasOne = make(map[string]*Model)
			}
			rel.HasOne[name] = model
			continue
		}

		if _, ok := model.Table.HasMany[name]; ok {
			model, err = NewModelPath(value, path[:i+1])
			if err != nil {
				return err
			}
			rel.HasMany = append(rel.HasMany, model)
			continue
		}

		return fmt.Errorf("pg: %s doesn't have %s relation", model.Table.Name, name)
	}

	return nil
}

func (rel *Relation) NewRecord() interface{} {
	rel.Model.NewRecord()
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

func (rel *Relation) LoadColumn(colIdx int, colName string, b []byte) error {
	modelName, colName := splitColumn(colName)
	model, ok := rel.HasOne[modelName]
	if !ok {
		model = rel.Model
	}
	return model.LoadColumn(colIdx, colName, b)
}
