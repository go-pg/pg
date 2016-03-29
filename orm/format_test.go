package orm_test

import (
	"fmt"
	"math"
	"testing"

	"gopkg.in/pg.v4/orm"
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

type formatTest struct {
	q       string
	args    []interface{}
	wanted  string
	wanterr string
}

type args []interface{}

var (
	structv         = &StructFormatter{String: "value"}
	embeddedStructv = &EmbeddedStructFormatter{structv}
)

var formatTests = []formatTest{
	{q: "?", wanted: "?"},
	{q: "?", args: args{uint64(math.MaxUint64)}, wanted: "18446744073709551615"},
	{q: "?", args: args{orm.Q("query")}, wanted: "query"},
	{q: "?", args: args{orm.F("field")}, wanted: `"field"`},
	{q: "?null_empty", args: args{structv}, wanted: `NULL`},
	{q: "?", args: args{structv}, wanted: `'{"String":"value","NullEmpty":""}'`},
	{q: `\? ?`, args: args{1}, wanted: "? 1"},
	{q: "? ?string ?", args: args{"one", "two", structv}, wanted: "'one' 'value' 'two'"},
	{q: "?string ?Method", args: args{structv}, wanted: "'value' 'method_value'"},
	{q: "?string ?Method ?Method2", args: args{embeddedStructv}, wanted: "'hello' 'method_value' 'method_value2'"},
	{q: "? ? ?", args: args{"foo", "bar"}, wanterr: "pg: expected at least 3 parameters, got 2"},
	{q: "?bar", args: args{structv}, wanterr: `pg: can't map "bar" on orm_test.StructFormatter`},
	{q: "?MethodWithArgs", args: args{structv}, wanterr: `pg: can't map "MethodWithArgs" on orm_test.StructFormatter`},
	{q: "?MethodWithCompositeReturn", args: args{structv}, wanterr: `pg: can't map "MethodWithCompositeReturn" on orm_test.StructFormatter`},
}

func TestFormatQuery(t *testing.T) {
	for i, test := range formatTests {
		got, err := orm.FormatQuery(test.q, test.args...)
		if test.wanterr != "" {
			if err == nil {
				t.Fatalf("expected error (q=%q args=%v)", test.q, test.args)
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
			t.Fatalf("got %q, wanted %q (q=%q args=%v)", got, test.wanted, test.q, test.args)
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
