package orm

import (
	"fmt"

	"gopkg.in/pg.v4/internal/parser"
	"gopkg.in/pg.v4/types"
)

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
		return Formatter{}.Append(dst, src, params...)
	default:
		return nil, fmt.Errorf("pg: can't append %T", src)
	}
}

func FormatQuery(query string, params ...interface{}) ([]byte, error) {
	if len(params) == 0 {
		return []byte(query), nil
	}
	return Formatter{}.Append(nil, query, params...)
}

func Q(s string, params ...interface{}) types.Q {
	q, err := FormatQuery(s, params...)
	if err != nil {
		panic(err)
	}
	return q
}

func F(s string, params ...interface{}) types.F {
	b, err := FormatQuery(s, params...)
	if err != nil {
		panic(err)
	}
	return types.F(b)
}

type Formatter struct {
	paramsMap map[string]interface{}
}

func (f *Formatter) SetParam(key string, value interface{}) {
	if f.paramsMap == nil {
		f.paramsMap = make(map[string]interface{})
	}
	f.paramsMap[key] = value
}

func (f Formatter) Q(query string, params ...interface{}) types.Q {
	b, err := f.Append(nil, query, params...)
	if err != nil {
		panic(err)
	}
	return types.Q(b)
}

func (f Formatter) Append(dst []byte, src string, params ...interface{}) ([]byte, error) {
	return f.AppendBytes(dst, []byte(src), params...)
}

func (f Formatter) AppendBytes(dst, src []byte, params ...interface{}) ([]byte, error) {
	var err error
	var paramsIndex int
	var model *StructModel
	var modelErr error

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
			if f.paramsMap != nil {
				if param, ok := f.paramsMap[name]; ok {
					dst = types.Append(dst, param, 1)
					continue
				}
			}

			if modelErr != nil {
				goto restore_param
			}

			if model == nil {
				if len(params) == 0 {
					goto restore_param
				}

				model, modelErr = NewStructModel(params[len(params)-1])
				if modelErr != nil {
					goto restore_param
				}
				params = params[:len(params)-1]
			}

			dst, ok = model.AppendParam(dst, name)
			if ok {
				continue
			}

		restore_param:
			dst = append(dst, '?')
			dst = append(dst, name...)
			continue
		}

		if paramsIndex >= len(params) {
			dst = append(dst, '?')
			continue
		}

		param := params[paramsIndex]
		switch param := param.(type) {
		case types.Q:
			dst, err = f.AppendBytes(dst, param)
			if err != nil {
				return nil, err
			}
		case types.F:
			// TODO: reuse memory
			field, err := param.AppendValue(nil, 1)
			if err != nil {
				return nil, err
			}

			dst, err = f.AppendBytes(dst, field)
			if err != nil {
				return nil, err
			}
		default:
			dst = types.Append(dst, param, 1)
		}
		paramsIndex++
	}

	return dst, nil
}
