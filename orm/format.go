package orm

import (
	"bytes"
	"strconv"
	"strings"

	"gopkg.in/pg.v5/internal/parser"
	"gopkg.in/pg.v5/types"
)

type FormatAppender interface {
	AppendFormat([]byte, QueryFormatter) []byte
}

//------------------------------------------------------------------------------

type queryParamsAppender struct {
	query  string
	params []interface{}
}

var _ FormatAppender = (*queryParamsAppender)(nil)

func Q(query string, params ...interface{}) FormatAppender {
	return queryParamsAppender{query, params}
}

func (q queryParamsAppender) AppendFormat(b []byte, f QueryFormatter) []byte {
	return f.FormatQuery(b, q.query, q.params...)
}

//------------------------------------------------------------------------------

type fieldAppender struct {
	field string
}

var _ FormatAppender = (*fieldAppender)(nil)

func (a fieldAppender) AppendFormat(b []byte, f QueryFormatter) []byte {
	return types.AppendField(b, a.field, 1)
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

		if id, numeric := p.ReadIdentifier(); id != nil {
			if numeric {
				idx, err := strconv.Atoi(string(id))
				if err != nil {
					goto restore_param
				}

				if idx >= len(params) {
					goto restore_param
				}

				dst = f.appendParam(dst, params[idx])
				continue
			}

			if f.paramsMap != nil {
				if param, ok := f.paramsMap[string(id)]; ok {
					dst = f.appendParam(dst, param)
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

			dst, ok = model.AppendParam(dst, string(id))
			if ok {
				continue
			}

		restore_param:
			dst = append(dst, '?')
			dst = append(dst, id...)
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

func (f Formatter) appendParam(b []byte, param interface{}) []byte {
	if fa, ok := param.(FormatAppender); ok {
		return fa.AppendFormat(b, f)
	}
	return types.Append(b, param, 1)
}
