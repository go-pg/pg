package orm

import (
	"fmt"
	"strings"
)

type Scope struct {
	Model *Model
	Joins []Join
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

func (s *Scope) GetJoin(name string) (join Join, ok bool) {
	for _, join = range s.Joins {
		if join.Relation.Field.SQLName == name {
			ok = true
			return
		}
	}
	return
}

func (s *Scope) Join(name string) error {
	path := strings.Split(name, ".")
	var goPath []string

	baseModel := s.Model
	joinModel := baseModel
	var rel *Relation
	var retErr error

	for _, name := range path {
		if v, ok := joinModel.Table.Relations[name]; ok {
			rel = v
			goPath = append(goPath, rel.Field.GoName)

			if v, ok := s.GetJoin(name); ok {
				baseModel = joinModel
				joinModel = v.JoinModel
				continue
			}

			model, err := NewModelPath(s.Model.Value(), goPath, rel.Join)
			if err != nil {
				retErr = err
				break
			}

			baseModel = joinModel
			joinModel = model
			continue
		}

		retErr = fmt.Errorf("pg: %s doesn't have %s relation", baseModel.Table.Name, name)
		break
	}

	if joinModel == baseModel {
		return retErr
	}

	var columns []string
	switch len(path) - len(goPath) {
	case 0:
		// ok
	default:
		columns = path[len(path)-1:]
	}

	s.Joins = append(s.Joins, Join{
		BaseModel: baseModel,
		JoinModel: joinModel,
		Relation:  rel,
		Columns:   columns,
	})

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
	join, ok := s.GetJoin(modelName)
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
