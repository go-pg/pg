package types_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/go-pg/pg/types"
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
		got := types.AppendJSONB(nil, []byte(test.s), 0)
		if !bytes.Equal(got, []byte(test.wanted)) {
			t.Errorf("got %q, wanted %q", got, test.wanted)
		}
	}
}

func BenchmarkAppendJSONB(b *testing.B) {
	bytes, err := json.Marshal(jsonbTests)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 1024)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = types.AppendJSONB(buf[:0], bytes, 1)
	}
}
