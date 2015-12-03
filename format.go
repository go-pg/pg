package pg

import (
	"errors"
	"fmt"

	"gopkg.in/pg.v3/internal/parser"
	"gopkg.in/pg.v3/orm"
	"gopkg.in/pg.v3/types"
)

func AppendQuery(dst []byte, src string, params ...interface{}) ([]byte, error) {
	return formatQuery(dst, []byte(src), params)
}

func FormatQuery(query string, params ...interface{}) (Q, error) {
	b, err := AppendQuery(nil, query, params...)
	if err != nil {
		return "", err
	}
	return Q(b), nil
}

func formatQuery(dst, src []byte, params []interface{}) ([]byte, error) {
	if len(params) == 0 {
		return append(dst, src...), nil
	}

	var model *orm.Model
	var paramInd int

	p := parser.NewParser(src)
	for p.Valid() {
		ch := p.Next()
		if ch == '\\' {
			if p.Peek() == '?' {
				p.SkipNext()
				dst = append(dst, '?')
				continue
			}
		} else if ch != '?' {
			dst = append(dst, ch)
			continue
		}

		name := p.ReadName()
		if name != "" {
			// Lazily initialize Model.
			if model == nil {
				if len(params) == 0 {
					return nil, errors.New("pg: expected at least one parameter, got nothing")
				}
				last := params[len(params)-1]
				params = params[:len(params)-1]

				var err error
				model, err = orm.NewModel(last)
				if err != nil {
					return nil, err
				}
			}
			var err error
			dst, err = model.AppendParam(dst, name)
			if err != nil {
				return nil, err
			}
		} else {
			if paramInd >= len(params) {
				return nil, fmt.Errorf(
					"pg: expected at least %d parameters, got %d",
					paramInd+1, len(params),
				)
			}

			dst = types.Append(dst, params[paramInd], true)
			paramInd++
		}
	}

	if paramInd < len(params) {
		return nil, fmt.Errorf("pg: expected %d parameters, got %d", paramInd, len(params))
	}

	return dst, nil
}
