package orm

import (
	"bytes"
	"strings"

	"gopkg.in/pg.v4/internal/parser"
	"gopkg.in/pg.v4/types"
)

type SQL struct {
	query  string
	params []interface{}
}

var _ types.ValueAppender = (*SQL)(nil)

func NewSQL(query string, params ...interface{}) *SQL {
	return &SQL{
		query:  query,
		params: params,
	}
}

func (q SQL) String() string {
	b, _ := q.AppendValue(nil, 1)
	return string(b)
}

func (q SQL) AppendValue(dst []byte, quote int) ([]byte, error) {
	return Formatter{}.Append(dst, q.query, q.params...), nil
}

func (q SQL) AppendFormat(dst []byte, f QueryFormatter) []byte {
	return f.FormatQuery(dst, q.query, q.params...)
}

//------------------------------------------------------------------------------

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

//------------------------------------------------------------------------------

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
	if (params == nil && f.paramsMap == nil) || strings.IndexByte(src, '?') == -1 {
		return append(dst, src...)
	}
	return f.append(dst, parser.NewString(src), params)
}

func (f Formatter) AppendBytes(dst, src []byte, params ...interface{}) []byte {
	if (params == nil && f.paramsMap == nil) || bytes.IndexByte(src, '?') == -1 {
		return append(dst, src...)
	}
	return f.append(dst, parser.New(src), params)
}

func (f Formatter) append(dst []byte, p *parser.Parser, params []interface{}) []byte {
	var paramsIndex int
	var model tableModel
	var modelErr error

	for p.Valid() {
		b, ok := p.ReadSep('?')
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

				model, modelErr = newTableModel(params[len(params)-1])
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
