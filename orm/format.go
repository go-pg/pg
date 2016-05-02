package orm

import (
	"gopkg.in/pg.v4/internal/parser"
	"gopkg.in/pg.v4/types"
)

func Q(query string, params ...interface{}) types.Q {
	if len(params) == 0 {
		return types.Q(query)
	}
	return Formatter{}.Append(nil, query, params...)
}

func F(field string, params ...interface{}) types.F {
	if len(params) == 0 {
		return types.F(field)
	}
	return types.F(Formatter{}.Append(nil, field, params...))
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

func (f Formatter) Append(dst []byte, src string, params ...interface{}) []byte {
	return f.append(dst, parser.NewString(src), params)
}

func (f Formatter) AppendBytes(dst, src []byte, params ...interface{}) []byte {
	return f.append(dst, parser.New(src), params)
}

// TODO: add formatContext and split this method
func (f Formatter) append(dst []byte, p *parser.Parser, params []interface{}) []byte {
	var paramsIndex int
	var model *structTableModel
	var modelErr error

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

				model, modelErr = newStructTableModel(params[len(params)-1])
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
		paramsIndex++

		dst = types.Append(dst, param, 1)
	}

	return dst
}
