package types_test

import (
	"bytes"
	"testing"

	"gopkg.in/pg.v4/types"
)

var jsonbTests = []struct {
	s, wanted string
}{
	{`\u0000`, `\\u0000`},
	{`\\u0000`, `\\u0000`},
	{`\\\u0000`, `\\\\u0000`},
	{`foo \u0000 bar`, `foo \\u0000 bar`},
	{`\u0001`, `\u0001`},
	{`\\u0001`, `\\u0001`},
}

func TestAppendJSONB(t *testing.T) {
	for _, test := range jsonbTests {
		got := types.AppendJSONB(nil, []byte(test.s), false)
		if !bytes.Equal(got, []byte(test.wanted)) {
			t.Errorf("got %q, wanted %q", got, test.wanted)
		}
	}
}
