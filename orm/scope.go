package orm

import (
	"fmt"
	"strings"
)

type Scope struct {
	Model *Model
	Joins []*Join
}

var (
	_ Collection    = (*Scope)(nil)
	_ ColumnScanner = (*Scope)(nil)
)

func NewScope(vi interface{}) (*Scope, error) {
	switch v := vi.(type) {
	case *Scope:
		return v, nil
	case *Model:
		return &Scope{
			Model: v,
		}, nil
	default:
		model, err := NewModel(vi)
		if err != nil {
			return nil, err
		}
		return &Scope{
			Model: model,
		}, nil
	}
}

func (s *Scope) getJoin(name string) (*Join, bool) {
	for _, join := range s.Joins {
		if join.Relation.Field.SQLName == name {
			return join, true
		}
	}
	return nil, false
}

func (s *Scope) Join(name string) error {
	path := strings.Split(name, ".")
	var goPath []string

	join := &Join{
		BaseModel: s.Model,
		JoinModel: s.Model,
	}
	var retErr error

	for _, name := range path {
		rel, ok := join.JoinModel.Table.Relations[name]
		if !ok {
			retErr = fmt.Errorf("pg: %s doesn't have %s relation", join.BaseModel.Table.Name, name)
			break
		}
		join.Relation = rel

		goPath = append(goPath, rel.Field.GoName)

		if v, ok := s.getJoin(name); ok {
			join.BaseModel = v.BaseModel
			join.JoinModel = v.JoinModel
			continue
		}

		model, err := NewModelPath(s.Model.Value(), goPath, rel.Join)
		if err != nil {
			retErr = err
			break
		}

		join.BaseModel = join.JoinModel
		join.JoinModel = model
	}

	if join.JoinModel == join.BaseModel {
		return retErr
	}

	if v, ok := s.getJoin(join.Relation.Field.SQLName); ok {
		join = v
	} else {
		s.Joins = append(s.Joins, join)
	}

	switch len(path) - len(goPath) {
	case 0:
		// ok
	default:
		join.Columns = append(join.Columns, path[len(path)-1])
	}

	return nil
}

func (s *Scope) AppendParam(b []byte, name string) ([]byte, error) {
	return s.Model.AppendParam(b, name)
}

func (s *Scope) NextModel() interface{} {
	s.Model.NextModel()
	for _, join := range s.Joins {
		if !join.Relation.Many {
			join.JoinModel.Bind(s.Model.strct)
		}
	}
	return s
}

func (s *Scope) ScanColumn(colIdx int, colName string, b []byte) error {
	modelName, colName := splitColumn(colName)
	join, ok := s.getJoin(modelName)
	if ok {
		return join.JoinModel.ScanColumn(colIdx, colName, b)
	}
	return s.Model.ScanColumn(colIdx, colName, b)
}

func splitColumn(s string) (string, string) {
	parts := strings.SplitN(s, "__", 2)
	if len(parts) != 2 {
		return "", s
	}
	return parts[0], parts[1]
}
