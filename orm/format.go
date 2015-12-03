package orm

import (
	"errors"
	"fmt"

	"gopkg.in/pg.v4/internal/parser"
	"gopkg.in/pg.v4/types"
)

func FormatQuery(query string, params ...interface{}) (types.Q, error) {
	b, err := AppendQuery(nil, query, params...)
	if err != nil {
		return "", err
	}
	return types.Q(b), nil
}

func AppendQuery(dst []byte, src interface{}, params ...interface{}) (b []byte, retErr error) {
	defer func() {
		if v := recover(); v != nil {
			if err, ok := v.(error); ok {
				retErr = err
			} else {
				retErr = fmt.Errorf("recovered from %q", v)
			}
		}
	}()
	switch src := src.(type) {
	case QueryAppender:
		return src.AppendQuery(dst, params...)
	case string:
		return Format(dst, src, params...)
	default:
		return nil, fmt.Errorf("pg: can't append %T", src)
	}
}

func Format(dst []byte, src string, params ...interface{}) ([]byte, error) {
	if len(params) == 0 {
		return append(dst, src...), nil
	}
	f := NewFormatter(params)
	return f.Append(dst, src)
}

func Q(s string, params ...interface{}) types.Q {
	q, err := FormatQuery(s, params...)
	if err != nil {
		panic(err)
	}
	return q
}

func F(s string, params ...interface{}) types.F {
	b, err := Format(nil, s, params...)
	if err != nil {
		panic(err)
	}
	return types.F(b)
}

type Formatter struct {
	params     []interface{}
	paramIndex int
	model      *TableModel
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
			if f.model == nil {
				if len(f.params) == 0 {
					return nil, errors.New("pg: expected at least one parameter, got nothing")
				}
				last := f.params[len(f.params)-1]
				f.params = f.params[:len(f.params)-1]

				var err error
				f.model, err = NewTableModel(last)
				if err != nil {
					return nil, err
				}
			}

			var err error
			dst, err = f.model.AppendParam(dst, name)
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
