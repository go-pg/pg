package orm

import (
	"fmt"

	"gopkg.in/pg.v4/internal/parser"
	"gopkg.in/pg.v4/types"
)

func AppendQuery(dst []byte, src interface{}, params ...interface{}) (b []byte, retErr error) {
	switch src := src.(type) {
	case QueryAppender:
		return src.AppendQuery(dst, params)
	case string:
		return Formatter{}.Append(dst, src, params, true), nil
	default:
		return nil, fmt.Errorf("pg: can't append %T", src)
	}
}

func Q(query string, params ...interface{}) types.Q {
	if len(params) == 0 {
		return types.Q(query)
	}
	return Formatter{}.Append(nil, query, params, true)
}

func F(field string, params ...interface{}) types.F {
	if len(params) == 0 {
		return types.F(field)
	}
	return types.F(Formatter{}.Append(nil, field, params, true))
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

func (f Formatter) Append(dst []byte, src string, params []interface{}, escape bool) []byte {
	return f.append(dst, parser.NewString(src), params, escape)
}

func (f Formatter) AppendBytes(dst, src []byte, params []interface{}, escape bool) []byte {
	return f.append(dst, parser.New(src), params, escape)
}

// TODO: add formatContext and split this method
func (f Formatter) append(dst []byte, p *parser.Parser, params []interface{}, escape bool) []byte {
	var paramsIndex int
	var model *StructModel
	var modelErr error
	var buf []byte

	for p.Valid() {
		b, ok := p.JumpTo('?')
		if !ok {
			dst = append(dst, b...)
			continue
		}
		if len(b) > 0 && b[len(b)-1] == '\\' {
			if escape {
				dst = append(dst, b[:len(b)-1]...)
			} else {
				dst = append(dst, b...)
			}
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

			dst, ok = model.FormatParam(f, dst, buf[:0], name)
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
		paramsIndex++

		switch param := param.(type) {
		case types.Q:
			buf = types.Append(buf[:0], param, 1)
			dst = f.append(dst, parser.New(buf), nil, false)
		case types.F:
			buf = types.Append(buf[:0], param, 1)
			dst = f.append(dst, parser.New(buf), nil, false)
		default:
			dst = types.Append(dst, param, 1)
		}
	}

	return dst
}
