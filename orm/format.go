package orm

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/go-pg/pg/internal/parser"
	"github.com/go-pg/pg/types"
)

var formatter Formatter

type FormatAppender interface {
	AppendFormat([]byte, QueryFormatter) []byte
}

type sepFormatAppender interface {
	FormatAppender
	AppendSep([]byte) []byte
}

//------------------------------------------------------------------------------

type queryParamsAppender struct {
	query  string
	params []interface{}
}

var _ FormatAppender = (*queryParamsAppender)(nil)

func Q(query string, params ...interface{}) queryParamsAppender {
	return queryParamsAppender{query, params}
}

func (q queryParamsAppender) AppendFormat(b []byte, f QueryFormatter) []byte {
	return f.FormatQuery(b, q.query, q.params...)
}

func (q queryParamsAppender) AppendValue(b []byte, quote int) ([]byte, error) {
	return q.AppendFormat(b, formatter), nil
}

//------------------------------------------------------------------------------

type whereAppender struct {
	conj   string
	query  string
	params []interface{}
}

var _ FormatAppender = (*whereAppender)(nil)

func (q whereAppender) AppendSep(b []byte) []byte {
	return append(b, q.conj...)
}

func (q whereAppender) AppendFormat(b []byte, f QueryFormatter) []byte {
	b = append(b, '(')
	b = f.FormatQuery(b, q.query, q.params...)
	b = append(b, ')')
	return b
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
	namedParams map[string]interface{}
}

func (f Formatter) String() string {
	if len(f.namedParams) == 0 {
		return ""
	}

	var keys []string
	for k, _ := range f.namedParams {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var ss []string
	for _, k := range keys {
		ss = append(ss, fmt.Sprintf("%s=%v", k, f.namedParams[k]))
	}
	return " " + strings.Join(ss, " ")
}

func (f Formatter) Copy() Formatter {
	var cp Formatter
	for param, value := range f.namedParams {
		cp.SetParam(param, value)
	}
	return cp
}

func (f *Formatter) SetParam(param string, value interface{}) {
	if f.namedParams == nil {
		f.namedParams = make(map[string]interface{})
	}
	f.namedParams[param] = value
}

func (f *Formatter) WithParam(param string, value interface{}) Formatter {
	cp := f.Copy()
	cp.SetParam(param, value)
	return cp
}

func (f Formatter) Append(dst []byte, src string, params ...interface{}) []byte {
	if (params == nil && f.namedParams == nil) || strings.IndexByte(src, '?') == -1 {
		return append(dst, src...)
	}
	return f.append(dst, parser.NewString(src), params)
}

func (f Formatter) AppendBytes(dst, src []byte, params ...interface{}) []byte {
	if (params == nil && f.namedParams == nil) || bytes.IndexByte(src, '?') == -1 {
		return append(dst, src...)
	}
	return f.append(dst, parser.New(src), params)
}

func (f Formatter) FormatQuery(dst []byte, query string, params ...interface{}) []byte {
	return f.Append(dst, query, params...)
}

func (f Formatter) append(dst []byte, p *parser.Parser, params []interface{}) []byte {
	var paramsIndex int
	var namedParams *tableParams
	var namedParamsInit bool
	var model tableModel

	if len(params) > 0 {
		var ok bool
		model, ok = params[len(params)-1].(tableModel)
		if ok {
			params = params[:len(params)-1]
		}
	}

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

		if id, numeric := p.ReadIdentifier(); id != "" {
			if numeric {
				idx, err := strconv.Atoi(id)
				if err != nil {
					goto restore_param
				}

				if idx >= len(params) {
					goto restore_param
				}

				dst = f.appendParam(dst, params[idx])
				continue
			}

			if f.namedParams != nil {
				if param, ok := f.namedParams[id]; ok {
					dst = f.appendParam(dst, param)
					continue
				}
			}

			if !namedParamsInit && len(params) > 0 {
				namedParams, ok = newTableParams(params[len(params)-1])
				if ok {
					params = params[:len(params)-1]
				}
				namedParamsInit = true
			}

			if namedParams != nil {
				dst, ok = namedParams.AppendParam(dst, id)
				if ok {
					continue
				}
			}

			if model != nil {
				dst, ok = model.AppendParam(dst, id)
				if ok {
					continue
				}
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
