package orm

import (
	"bytes"
	"strings"

	"gopkg.in/pg.v4/internal/parser"
	"gopkg.in/pg.v4/types"
)

type FormatAppender interface {
	AppendFormat([]byte, QueryFormatter) []byte
}

//------------------------------------------------------------------------------

type queryParams struct {
	query  string
	params []interface{}
}

var _ FormatAppender = (*queryParams)(nil)

func Q(query string, params ...interface{}) FormatAppender {
	return queryParams{query, params}
}

func (q queryParams) AppendFormat(dst []byte, f QueryFormatter) []byte {
	return f.FormatQuery(dst, q.query, q.params...)
}

//------------------------------------------------------------------------------

type fieldParams struct {
	field  string
	params []interface{}
}

var _ FormatAppender = (*fieldParams)(nil)

func F(field string, params ...interface{}) FormatAppender {
	return fieldParams{field, params}
}

func (q fieldParams) AppendFormat(b []byte, f QueryFormatter) []byte {
	return types.AppendField(b, q.field, 1)
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

func (f Formatter) FormatQuery(dst []byte, query string, params ...interface{}) []byte {
	return f.Append(dst, query, params...)
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

		if fa, ok := param.(FormatAppender); ok {
			dst = fa.AppendFormat(dst, f)
		} else {
			dst = types.Append(dst, param, 1)
		}
	}

	return dst
}
