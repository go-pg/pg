package orm

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/go-pg/pg/internal"
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
var _ types.ValueAppender = (*queryParamsAppender)(nil)

func Q(query string, params ...interface{}) *queryParamsAppender {
	return &queryParamsAppender{query, params}
}

func (q *queryParamsAppender) AppendFormat(b []byte, f QueryFormatter) []byte {
	return f.FormatQuery(b, q.query, q.params...)
}

func (q *queryParamsAppender) AppendValue(b []byte, quote int) []byte {
	return q.AppendFormat(b, formatter)
}

func (q *queryParamsAppender) Value() types.Q {
	b := q.AppendValue(nil, 1)
	return types.Q(internal.BytesToString(b))
}

//------------------------------------------------------------------------------

type condGroupAppender struct {
	sep  string
	cond []sepFormatAppender
}

var _ FormatAppender = (*condAppender)(nil)
var _ sepFormatAppender = (*condAppender)(nil)

func (q *condGroupAppender) AppendSep(b []byte) []byte {
	return append(b, q.sep...)
}

func (q *condGroupAppender) AppendFormat(b []byte, f QueryFormatter) []byte {
	b = append(b, '(')
	for i, app := range q.cond {
		if i > 0 {
			b = app.AppendSep(b)
		}
		b = app.AppendFormat(b, f)
	}
	b = append(b, ')')
	return b
}

//------------------------------------------------------------------------------

type condAppender struct {
	sep    string
	cond   string
	params []interface{}
}

var _ FormatAppender = (*condAppender)(nil)
var _ sepFormatAppender = (*condAppender)(nil)

func (q *condAppender) AppendSep(b []byte) []byte {
	return append(b, q.sep...)
}

func (q *condAppender) AppendFormat(b []byte, f QueryFormatter) []byte {
	b = append(b, '(')
	b = f.FormatQuery(b, q.cond, q.params...)
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

func (f Formatter) copy() Formatter {
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
	cp := f.copy()
	cp.SetParam(param, value)
	return cp
}

func (f Formatter) Param(param string) interface{} {
	return f.namedParams[param]
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
	var namedParamsOnce bool
	var tableParams *tableParams
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
				param, paramOK := f.namedParams[id]
				if paramOK {
					dst = f.appendParam(dst, param)
					continue
				}
			}

			if !namedParamsOnce && len(params) > 0 {
				namedParamsOnce = true
				tableParams, _ = newTableParams(params[len(params)-1])
			}

			if tableParams != nil {
				dst, ok = tableParams.AppendParam(dst, f, id)
				if ok {
					continue
				}
			}

			if model != nil {
				dst, ok = model.AppendParam(dst, f, id)
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

		dst = f.appendParam(dst, param)
	}

	return dst
}

type queryAppender interface {
	AppendQuery(dst []byte) ([]byte, error)
}

func (f Formatter) appendParam(b []byte, param interface{}) []byte {
	switch param := param.(type) {
	case queryAppender:
		bb, err := param.AppendQuery(b)
		if err != nil {
			return types.AppendError(b, err)
		}
		return bb
	case FormatAppender:
		return param.AppendFormat(b, f)
	default:
		return types.Append(b, param, 1)
	}
}
