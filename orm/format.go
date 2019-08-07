package orm

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/whenspeakteam/pg/v9/internal"
	"github.com/whenspeakteam/pg/v9/internal/parser"
	"github.com/whenspeakteam/pg/v9/types"
)

var defaultFmter Formatter

type queryWithSepAppender interface {
	QueryAppender
	AppendSep([]byte) []byte
}

//------------------------------------------------------------------------------

type queryParamsAppender struct {
	query  string
	params []interface{}
}

var _ QueryAppender = (*queryParamsAppender)(nil)
var _ types.ValueAppender = (*queryParamsAppender)(nil)

//nolint
func Q(query string, params ...interface{}) *queryParamsAppender {
	return &queryParamsAppender{query, params}
}

func (q *queryParamsAppender) AppendQuery(fmter QueryFormatter, b []byte) ([]byte, error) {
	return fmter.FormatQuery(b, q.query, q.params...), nil
}

func (q *queryParamsAppender) AppendValue(b []byte, quote int) ([]byte, error) {
	return q.AppendQuery(defaultFmter, b)
}

func (q *queryParamsAppender) Value() types.Q {
	b, err := q.AppendValue(nil, 1)
	if err != nil {
		return types.Q(err.Error())
	}
	return types.Q(internal.BytesToString(b))
}

//------------------------------------------------------------------------------

type condGroupAppender struct {
	sep  string
	cond []queryWithSepAppender
}

var _ QueryAppender = (*condAppender)(nil)
var _ queryWithSepAppender = (*condAppender)(nil)

func (q *condGroupAppender) AppendSep(b []byte) []byte {
	return append(b, q.sep...)
}

func (q *condGroupAppender) AppendQuery(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	b = append(b, '(')
	for i, app := range q.cond {
		if i > 0 {
			b = app.AppendSep(b)
		}
		b, err = app.AppendQuery(fmter, b)
		if err != nil {
			return nil, err
		}
	}
	b = append(b, ')')
	return b, nil
}

//------------------------------------------------------------------------------

type condAppender struct {
	sep    string
	cond   string
	params []interface{}
}

var _ QueryAppender = (*condAppender)(nil)
var _ queryWithSepAppender = (*condAppender)(nil)

func (q *condAppender) AppendSep(b []byte) []byte {
	return append(b, q.sep...)
}

func (q *condAppender) AppendQuery(fmter QueryFormatter, b []byte) ([]byte, error) {
	b = append(b, '(')
	b = fmter.FormatQuery(b, q.cond, q.params...)
	b = append(b, ')')
	return b, nil
}

//------------------------------------------------------------------------------

type columnAppender struct {
	sqlName string
	column  types.Q
}

var _ QueryAppender = (*columnAppender)(nil)

func (a columnAppender) AppendQuery(fmter QueryFormatter, b []byte) ([]byte, error) {
	return append(b, a.column...), nil
}

//------------------------------------------------------------------------------

type fieldAppender struct {
	field string
}

var _ QueryAppender = (*fieldAppender)(nil)

func (a fieldAppender) AppendQuery(fmter QueryFormatter, b []byte) ([]byte, error) {
	return types.AppendField(b, a.field, 1), nil
}

//------------------------------------------------------------------------------

type dummyFormatter struct{}

func (f dummyFormatter) FormatQuery(b []byte, query string, params ...interface{}) []byte {
	return append(b, query...)
}

func isPlaceholderFormatter(fmter QueryFormatter) bool {
	if fmter == nil {
		return false
	}
	b := fmter.FormatQuery(nil, "?", 0)
	return bytes.Equal(b, []byte("?"))
}

//------------------------------------------------------------------------------

type Formatter struct {
	namedParams map[string]interface{}
	model       TableModel
}

func (f Formatter) String() string {
	if len(f.namedParams) == 0 {
		return ""
	}

	keys := make([]string, len(f.namedParams))
	index := 0
	for k := range f.namedParams {
		keys[index] = k
		index++
	}

	sort.Strings(keys)

	ss := make([]string, len(keys))
	for i, k := range keys {
		ss[i] = fmt.Sprintf("%s=%v", k, f.namedParams[k])
	}
	return " " + strings.Join(ss, " ")
}

func (f Formatter) clone() Formatter {
	var cp Formatter

	cp.model = f.model
	if len(f.namedParams) > 0 {
		cp.namedParams = make(map[string]interface{}, len(f.namedParams))
	}
	for param, value := range f.namedParams {
		cp.setParam(param, value)
	}

	return cp
}

func (f Formatter) WithTableModel(model TableModel) Formatter {
	cp := f.clone()
	cp.model = model
	return cp
}

func (f Formatter) WithModel(model interface{}) Formatter {
	switch model := model.(type) {
	case TableModel:
		return f.WithTableModel(model)
	case *Query:
		return f.WithTableModel(model.model)
	case queryCommand:
		return f.WithTableModel(model.Query().model)
	default:
		panic(fmt.Errorf("pg: unsupported model %T", model))
	}
}

func (f *Formatter) setParam(param string, value interface{}) {
	if f.namedParams == nil {
		f.namedParams = make(map[string]interface{})
	}
	f.namedParams[param] = value
}

func (f Formatter) WithParam(param string, value interface{}) Formatter {
	cp := f.clone()
	cp.setParam(param, value)
	return cp
}

func (f Formatter) Param(param string) interface{} {
	return f.namedParams[param]
}

func (f Formatter) hasParams() bool {
	return len(f.namedParams) > 0 || f.model != nil
}

func (f Formatter) FormatQueryBytes(dst, query []byte, params ...interface{}) []byte {
	if (params == nil && !f.hasParams()) || bytes.IndexByte(query, '?') == -1 {
		return append(dst, query...)
	}
	return f.append(dst, parser.New(query), params)
}

func (f Formatter) FormatQuery(dst []byte, query string, params ...interface{}) []byte {
	if (params == nil && !f.hasParams()) || strings.IndexByte(query, '?') == -1 {
		return append(dst, query...)
	}
	return f.append(dst, parser.NewString(query), params)
}

func (f Formatter) append(dst []byte, p *parser.Parser, params []interface{}) []byte {
	var paramsIndex int
	var namedParamsOnce bool
	var tableParams *tableParams

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

		id, numeric := p.ReadIdentifier()
		if id != "" {
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
				dst, ok = tableParams.AppendParam(f, dst, id)
				if ok {
					continue
				}
			}

			if f.model != nil {
				dst, ok = f.model.AppendParam(f, dst, id)
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

func (f Formatter) appendParam(b []byte, param interface{}) []byte {
	switch param := param.(type) {
	case QueryAppender:
		bb, err := param.AppendQuery(f, b)
		if err != nil {
			return types.AppendError(b, err)
		}
		return bb
	default:
		return types.Append(b, param, 1)
	}
}
