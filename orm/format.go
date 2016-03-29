package orm

import (
	"errors"
	"fmt"

	"gopkg.in/pg.v4/internal/parser"
	"gopkg.in/pg.v4/types"
)

func FormatQuery(query string, params ...interface{}) (types.Q, error) {
	b, err := AppendQuery(make([]byte, 0, len(query)), query, params...)
	if err != nil {
		return nil, err
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
	params      []interface{}
	paramsIndex int

	model     TableModel
	paramsMap map[string]interface{}
}

func NewFormatter(params []interface{}) *Formatter {
	return &Formatter{
		params: params,
	}
}

func (f *Formatter) SetParam(key string, value interface{}) {
	if f.paramsMap == nil {
		f.paramsMap = make(map[string]interface{})
	}
	f.paramsMap[key] = value
}

func (f *Formatter) Append(dst []byte, src string) ([]byte, error) {
	return f.AppendBytes(dst, []byte(src))
}

func (f *Formatter) AppendBytes(dst, src []byte) ([]byte, error) {
	var err error
	p := parser.New(src)

	for p.Valid() {
		b, ok := p.JumpTo('?')
		if !ok {
			dst = append(dst, b...)
			continue
		}
		if len(b) > 0 && b[len(b)-1] == '\\' {
			dst = append(dst, b[:len(b)-1]...)
			dst = append(dst, '?')
			continue
		}
		dst = append(dst, b...)

		if name := string(p.ReadIdentifier()); name != "" {
			dst, err = f.appendNamedParam(dst, name)
			if err != nil {
				return nil, err
			}
			continue
		}

		if f.paramsIndex >= len(f.params) {
			err := fmt.Errorf(
				"pg: expected at least %d parameters, got %d",
				f.paramsIndex+1, len(f.params),
			)
			return nil, err
		}

		dst = types.Append(dst, f.params[f.paramsIndex], 1)
		f.paramsIndex++
	}

	return dst, nil
}

func (f *Formatter) appendNamedParam(dst []byte, name string) ([]byte, error) {
	if f.paramsMap != nil {
		if param, ok := f.paramsMap[name]; ok {
			dst = types.Append(dst, param, 1)
			return dst, nil
		}
	}

	if f.model == nil {
		if err := f.initModel(); err != nil {
			return nil, err
		}
	}
	return f.model.AppendParam(dst, name)
}

func (f *Formatter) initModel() error {
	if len(f.params) == 0 {
		return errors.New("pg: expected at least one parameter, got nothing")
	}
	last := f.params[len(f.params)-1]
	f.params = f.params[:len(f.params)-1]

	var err error
	f.model, err = NewTableModel(last)
	return err
}
