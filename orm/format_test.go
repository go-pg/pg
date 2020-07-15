package orm_test

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/go-pg/pg/v10/orm"
	"github.com/go-pg/pg/v10/types"
)

type ValuerError string

func (e ValuerError) Value() (driver.Value, error) {
	return nil, errors.New(string(e))
}

type StructFormatter struct {
	tableName struct{} `pg:"my_name,alias:my_alias"`

	String  string
	UseZero string `pg:",use_zero"`
	Iface   interface{}
}

func (StructFormatter) Method() string {
	return "method_value"
}

func (StructFormatter) MethodParam() types.Safe {
	return "?string"
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
}

var (
	structv = &StructFormatter{
		String: "string_value",
		Iface:  "iface_value",
	}
	embeddedStructv = &EmbeddedStructFormatter{structv}
)

var formatTests = []formatTest{
	{q: "?", params: params{ValuerError("error")}, wanted: "?!(error)"},

	{q: "?", wanted: "?"},
	{q: "?_?", params: params{"foo", "bar"}, wanted: "'foo'_'bar'"},
	{q: "?0_?0", params: params{"foo", "bar"}, wanted: "'foo'_'foo'"},
	{q: "? ? ?", params: params{"foo", "bar"}, wanted: "'foo' 'bar' ?"},
	{q: "?0 ?1", params: params{"foo", "bar"}, wanted: "'foo' 'bar'"},
	{q: "?0 ?1 ?2", params: params{"foo", "bar"}, wanted: "'foo' 'bar' ?2"},
	{q: "?0 ?1 ?0", params: params{"foo", "bar"}, wanted: "'foo' 'bar' 'foo'"},

	{q: "one ?foo two", wanted: "one ?foo two"},
	{q: "one ?foo two", params: params{structv}, wanted: "one ?foo two"},
	{q: "one ?MethodWithArgs two", params: params{structv}, wanted: "one ?MethodWithArgs two"},
	{q: "one ?MethodWithCompositeReturn two", params: params{structv}, wanted: "one ?MethodWithCompositeReturn two"},

	{q: "?", params: params{uint64(math.MaxUint64)}, wanted: "18446744073709551615"},
	{q: "?", params: params{types.Safe("query")}, wanted: "query"},
	{q: "?", params: params{types.Ident("field")}, wanted: `"field"`},
	{q: "?", params: params{structv}, wanted: `'{"String":"string_value","UseZero":"","Iface":"iface_value"}'`},

	{q: `\? ?`, params: params{1}, wanted: "? 1"},
	{q: `?`, params: params{types.Safe(`\?`)}, wanted: `\?`},
	{q: `?`, params: params{types.Safe(`\\?`)}, wanted: `\\?`},
	{q: `?`, params: params{types.Safe(`\?param`)}, wanted: `\?param`},

	{q: "?string", params: params{structv}, wanted: `'string_value'`},
	{q: "?(string)", params: params{structv}, wanted: `'string_value'`},
	{q: "?iface", params: params{structv}, wanted: `'iface_value'`},
	{q: "?string", params: params{&StructFormatter{}}, wanted: `NULL`},
	{q: "?use_zero", params: params{&StructFormatter{}}, wanted: `''`},
	{
		q:      "? ?string ?",
		params: params{"one", "two", structv},
		wanted: "'one' 'string_value' 'two'",
	},
	{
		q:      "?string ?Method",
		params: params{structv},
		wanted: "'string_value' 'method_value'",
	},
	{
		q:      "?string ?Method ?Method2",
		params: params{embeddedStructv},
		wanted: "'string_value' 'method_value' 'method_value2'",
	},

	{
		q:         "?string",
		params:    params{structv},
		paramsMap: paramsMap{"string": "my_value"},
		wanted:    "'my_value'",
	},
	{
		q:         "?",
		params:    params{types.Safe("?string")},
		paramsMap: paramsMap{"string": "my_value"},
		wanted:    "?string",
	},
	{
		q:         "?",
		params:    params{types.Ident("?string")},
		paramsMap: paramsMap{"string": types.Safe("my_value")},
		wanted:    `"?string"`,
	},
	{
		q:         "?",
		params:    params{orm.SafeQuery("?string")},
		paramsMap: paramsMap{"string": "my_value"},
		wanted:    "'my_value'",
	},
	{
		q:         "?MethodParam",
		params:    params{structv},
		paramsMap: paramsMap{"string": "my_value"},
		wanted:    "?string",
	},
}

func TestFormatQuery(t *testing.T) {
	for i, test := range formatTests {
		f := orm.NewFormatter()
		for k, v := range test.paramsMap {
			f = f.WithParam(k, v)
		}

		got := f.FormatQuery(nil, test.q, test.params...)
		if string(got) != test.wanted {
			t.Fatalf(
				"#%d: got %q, wanted %q (q=%q params=%v paramsMap=%v)",
				i, got, test.wanted, test.q, test.params, test.paramsMap,
			)
		}
	}
}

func BenchmarkFormatQueryWithoutParams(b *testing.B) {
	var f orm.Formatter
	for i := 0; i < b.N; i++ {
		_ = f.FormatQuery(nil, "SELECT * FROM my_table WHERE id = 1")
	}
}

func BenchmarkFormatQuery1Param(b *testing.B) {
	var f orm.Formatter
	for i := 0; i < b.N; i++ {
		_ = f.FormatQuery(nil, "SELECT * FROM my_table WHERE id = ?", 1)
	}
}

func BenchmarkFormatQuery10Params(b *testing.B) {
	var f orm.Formatter
	for i := 0; i < b.N; i++ {
		_ = f.FormatQuery(
			nil, "SELECT * FROM my_table WHERE id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
	var f orm.Formatter
	param := StructFormatter{
		String: "1",
	}
	for i := 0; i < b.N; i++ {
		_ = f.FormatQuery(nil, "SELECT * FROM my_table WHERE id = ?string", param)
	}
}

func BenchmarkFormatQueryStructMethod(b *testing.B) {
	var f orm.Formatter
	param := StructFormatter{}
	for i := 0; i < b.N; i++ {
		_ = f.FormatQuery(nil, "SELECT * FROM my_table WHERE id = ?Method", &param)
	}
}
