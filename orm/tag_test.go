package orm

import (
	"testing"
)

var tagTests = []struct {
	tag  string
	name string
	opts map[string]string
}{
	{"", "", nil},

	{"hello", "hello", nil},
	{",hello", "", map[string]string{"hello": ""}},
	{"hello:world", "", map[string]string{"hello": "world"}},
	{"hello:'world1,world2'", "", map[string]string{"hello": "world1,world2"}},
}

func TestTagParser(t *testing.T) {
	for _, test := range tagTests {
		tag := parseTag(test.tag)
		if tag.Name != test.name {
			t.Fatalf("got %q, wanted %q", tag.Name, test.name)
		}

		if len(tag.Options) != len(test.opts) {
			t.Fatalf("got %d options, wanted %d", len(tag.Options), len(test.opts))
		}

		for k, v := range test.opts {
			if tag.Options[k] != v {
				t.Fatalf("got %s=%q, wanted %q", k, tag.Options[k], v)
			}
		}
	}
}
