package orm_test

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"testing"

	"gopkg.in/pg.v4/orm"
	"gopkg.in/pg.v4/types"
)

type ValuerError string

func (e ValuerError) Value() (driver.Value, error) {
	return nil, errors.New(string(e))
}

type StructFormatter struct {
	String    string
	NullEmpty string `pg:",nullempty"`
}

func (StructFormatter) Method() string {
	return "method_value"
}

func (StructFormatter) MethodParam() types.Q {
	return types.Q("?string")
}

func (StructFormatter) MethodWithArgs(string) string {
	return "method_value"
}

func (StructFormatter) MethodWithCompositeReturn() (string, string) {
	return "method_value1", "method_value2"
}

type EmbeddedStructFormatter struct {
	*StructFormatter
}

func (EmbeddedStructFormatter) Method2() string {
	return "method_value2"
}

type (
	params    []interface{}
	paramsMap map[string]interface{}
)

type formatTest struct {
	q         string
	params    params
	paramsMap paramsMap
	wanted    string
	wanterr   string
}

var (
	structv         = &StructFormatter{String: "field_value"}
	embeddedStructv = &EmbeddedStructFormatter{structv}
)

var formatTests = []formatTest{
	{q: "?", params: params{ValuerError("error")}, wanted: "?!(error)"},

	{q: "?", wanted: "?"},
	{q: "? ? ?", params: params{"foo", "bar"}, wanted: "'foo' 'bar' ?"},

	{q: "one ?foo two", wanted: "one ?foo two"},
	{q: "one ?foo two", params: params{structv}, wanted: "one ?foo two"},
	{q: "one ?MethodWithArgs two", params: params{structv}, wanted: "one ?MethodWithArgs two"},
	{q: "one ?MethodWithCompositeReturn two", params: params{structv}, wanted: "one ?MethodWithCompositeReturn two"},

	{q: "?", params: params{uint64(math.MaxUint64)}, wanted: "18446744073709551615"},
	{q: "?", params: params{orm.Q("query")}, wanted: "query"},
	{q: "?", params: params{orm.F("field")}, wanted: `"field"`},
	{q: "?", params: params{structv}, wanted: `'{"String":"field_value","NullEmpty":""}'`},

	{q: `\? ?`, params: params{1}, wanted: "? 1"},
	{q: `?`, params: params{`\?`}, wanted: `'\?'`},
	{q: `?`, params: params{`\?param`}, wanted: `'\?param'`},

	{q: "?null_empty", params: params{structv}, wanted: `NULL`},
	{q: "? ?string ?", params: params{"one", "two", structv}, wanted: "'one' 'field_value' 'two'"},
	{q: "?string ?Method", params: params{structv}, wanted: "'field_value' 'method_value'"},
	{q: "?string ?Method ?Method2", params: params{embeddedStructv}, wanted: "'field_value' 'method_value' 'method_value2'"},

	{q: "?string", params: params{structv}, paramsMap: paramsMap{"string": "my_value"}, wanted: "'my_value'"},

	{q: "?", params: params{types.Q("?string")}, paramsMap: paramsMap{"string": "my_value"}, wanted: "'my_value'"},
	{q: "?", params: params{types.F("?string")}, paramsMap: paramsMap{"string": types.Q("my_value")}, wanted: `"my_value"`},
	{q: "?MethodParam", params: params{structv}, paramsMap: paramsMap{"string": "my_value"}, wanted: "'my_value'"},
}

func TestFormatQuery(t *testing.T) {
	for _, test := range formatTests {
		var f orm.Formatter
		for k, v := range test.paramsMap {
			f.SetParam(k, v)
		}

		got := f.Append(nil, test.q, test.params...)
		if string(got) != test.wanted {
			t.Fatalf(
				"got %q, wanted %q (q=%q params=%v paramsMap=%v)",
				got, test.wanted, test.q, test.params, test.paramsMap,
			)
		}
	}
}

func BenchmarkFormatQueryWithoutParams(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = orm.FormatQuery("SELECT * FROM my_table WHERE id = 1")
	}
}

func BenchmarkFormatQuery1Param(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = orm.FormatQuery("SELECT * FROM my_table WHERE id = ?", 1)
	}
}

func BenchmarkFormatQuery10Params(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = orm.FormatQuery(
			"SELECT * FROM my_table WHERE id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
		)
	}
}

func BenchmarkFormatQuerySprintf(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("SELECT * FROM my_table WHERE id = %d", 1)
	}
}

func BenchmarkFormatQueryStructParam(b *testing.B) {
	param := StructFormatter{
		String: "1",
	}
	for i := 0; i < b.N; i++ {
		_ = orm.FormatQuery("SELECT * FROM my_table WHERE id = ?string", param)
	}
}

func BenchmarkFormatQueryStructMethod(b *testing.B) {
	param := StructFormatter{}
	for i := 0; i < b.N; i++ {
		_ = orm.FormatQuery("SELECT * FROM my_table WHERE id = ?Method", &param)
	}
}
