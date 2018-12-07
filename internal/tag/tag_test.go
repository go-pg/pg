package tag_test

import (
	"testing"

	"github.com/go-pg/pg/internal/tag"
)

var tagTests = []struct {
	tag  string
	name string
	opts map[string]string
}{
	{"", "", nil},

	{"hello", "hello", nil},
	{"hello,world", "hello", map[string]string{"world": ""}},
	{"'hello,world'", "'hello,world'", nil},
	{",hello", "", map[string]string{"hello": ""}},
	{",hello,world", "", map[string]string{"hello": "", "world": ""}},
	{"hello:", "", map[string]string{"hello": ""}},
	{"hello:world", "", map[string]string{"hello": "world"}},
	{"hello:world,foo", "", map[string]string{"hello": "world", "foo": ""}},
	{"hello:world,foo:bar", "", map[string]string{"hello": "world", "foo": "bar"}},
	{"hello:'world1,world2'", "", map[string]string{"hello": "'world1,world2'"}},
	{`hello:'D\'Angelo', foo:bar`, "", map[string]string{"hello": "'D'Angelo'", "foo": "bar"}},
}

func TestTagParser(t *testing.T) {
	for _, test := range tagTests {
		tag := tag.Parse(test.tag)
		if tag.Name != test.name {
			t.Fatalf("got %q, wanted %q (tag=%q)", tag.Name, test.name, test.tag)
		}

		if len(tag.Options) != len(test.opts) {
			t.Fatalf(
				"got %#v options, wanted %#v (tag=%q)",
				tag.Options, test.opts, test.tag,
			)
		}

		for k, v := range test.opts {
			s, ok := tag.Options[k]
			if !ok {
				t.Fatalf("option=%q does not exist (tag=%q)", k, test.tag)
			}
			if s != v {
				t.Fatalf("got %s=%q, wanted %q (tag=%q)", k, tag.Options[k], v, test.tag)
			}
		}
	}
}
