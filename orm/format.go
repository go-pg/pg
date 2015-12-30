package orm

import (
	"errors"
	"fmt"

	"gopkg.in/pg.v3/internal/parser"
	"gopkg.in/pg.v3/types"
)

type Formatter struct {
	params     []interface{}
	paramIndex int
	scope      *Scope
}

func NewFormatter(params []interface{}) *Formatter {
	return &Formatter{
		params: params,
	}
}

func (f *Formatter) Append(dst []byte, src string) ([]byte, error) {
	return f.AppendBytes(dst, []byte(src))
}

func (f *Formatter) AppendBytes(dst []byte, src []byte) ([]byte, error) {
	if f.params == nil {
		return append(dst, src...), nil
	}

	p := parser.New(src)

	for p.Valid() {
		ch := p.Read()
		if ch == '\\' {
			if p.Peek() == '?' {
				p.Skip('?')
				dst = append(dst, '?')
				continue
			}
		} else if ch != '?' {
			dst = append(dst, ch)
			continue
		}

		if name := string(p.ReadIdentifier()); name != "" {
			if f.scope == nil {
				if len(f.params) == 0 {
					return nil, errors.New("pg: expected at least one parameter, got nothing")
				}
				last := f.params[len(f.params)-1]
				f.params = f.params[:len(f.params)-1]

				var err error
				f.scope, err = NewScope(last)
				if err != nil {
					return nil, err
				}
			}

			var err error
			dst, err = f.scope.AppendParam(dst, name)
			if err != nil {
				return nil, err
			}

			continue
		}

		if f.paramIndex >= len(f.params) {
			err := fmt.Errorf(
				"pg: expected at least %d parameters, got %d",
				f.paramIndex+1, len(f.params),
			)
			return nil, err
		}

		dst = types.Append(dst, f.params[f.paramIndex], true)
		f.paramIndex++
	}

	return dst, nil
}
