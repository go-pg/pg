package orm_test

import (
	"fmt"
	"math"
	"testing"

	"gopkg.in/pg.v4/orm"
	"gopkg.in/pg.v4/types"
)

type StructFormatter struct {
	String    string
	NullEmpty string `pg:",nullempty"`
}

func (StructFormatter) Method() string {
	return "method_value"
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
	structv         = &StructFormatter{String: "value"}
	embeddedStructv = &EmbeddedStructFormatter{structv}
)

var formatTests = []formatTest{
	{q: "?", wanted: "?"},
	{q: "? ? ?", params: params{"foo", "bar"}, wanted: "'foo' 'bar' ?"},

	{q: "?foo", wanted: "?foo"},
	{q: "?foo", params: params{structv}, wanted: "?foo"},
	{q: "?MethodWithArgs", params: params{structv}, wanted: "?MethodWithArgs"},
	{q: "?MethodWithCompositeReturn", params: params{structv}, wanted: "?MethodWithCompositeReturn"},

	{q: "?", params: params{uint64(math.MaxUint64)}, wanted: "18446744073709551615"},
	{q: "?", params: params{orm.Q("query")}, wanted: "query"},
	{q: "?", params: params{orm.F("field")}, wanted: `"field"`},
	{q: "?", params: params{structv}, wanted: `'{"String":"value","NullEmpty":""}'`},
	{q: `\? ?`, params: params{1}, wanted: "? 1"},

	{q: "?null_empty", params: params{structv}, wanted: `NULL`},
	{q: "? ?string ?", params: params{"one", "two", structv}, wanted: "'one' 'value' 'two'"},
	{q: "?string ?Method", params: params{structv}, wanted: "'value' 'method_value'"},
	{q: "?string ?Method ?Method2", params: params{embeddedStructv}, wanted: "'value' 'method_value' 'method_value2'"},

	{q: "?string", params: params{structv}, paramsMap: paramsMap{"string": "my_value"}, wanted: "'my_value'"},
	{q: "?", params: params{types.Q("?string")}, paramsMap: paramsMap{"string": "my_value"}, wanted: "'my_value'"},
	{q: "?", params: params{types.F("?string")}, paramsMap: paramsMap{"string": types.Q("my_value")}, wanted: `"my_value"`},
}

func TestFormatQuery(t *testing.T) {
	for i, test := range formatTests {
		var f orm.Formatter
		for k, v := range test.paramsMap {
			f.SetParam(k, v)
		}

		got, err := f.Append(nil, test.q, test.params...)
		if test.wanterr != "" {
			if err == nil {
				t.Fatalf("expected error (q=%q params=%v)", test.q, test.params)
			}
			if err.Error() != test.wanterr {
				t.Fatalf("got %q, wanted %q", err.Error(), test.wanterr)
			}
			continue
		}
		if err != nil {
			t.Fatalf("test #%d failed: %s", i, err)
			continue
		}
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
		_, err := orm.FormatQuery("SELECT * FROM my_table WHERE id = 1")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFormatQuerySimpleParam(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := orm.FormatQuery("SELECT * FROM my_table WHERE id = ?", 1)
		if err != nil {
			b.Fatal(err)
		}
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
		_, err := orm.FormatQuery("SELECT * FROM my_table WHERE id = ?string", param)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFormatQueryStructMethod(b *testing.B) {
	param := StructFormatter{}
	for i := 0; i < b.N; i++ {
		_, err := orm.FormatQuery("SELECT * FROM my_table WHERE id = ?Method", &param)
		if err != nil {
			b.Fatal(err)
		}
	}
}
