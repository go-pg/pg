package types_test

import (
	"testing"

	"github.com/go-pg/pg/types"
)

func TestInOp(t *testing.T) {
	tests := []struct {
		app    types.ValueAppender
		wanted string
	}{
		{types.In(), ""},
		{types.In(1), "1"},
		{types.In(1, 2, 3), "1,2,3"},
		{types.In([]int{1, 2, 3}), "(1,2,3)"},
		{types.In([]int{1, 2}, []int{3, 4}), "(1,2),(3,4)"},
	}

	for _, test := range tests {
		b, err := test.app.AppendValue(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != test.wanted {
			t.Fatalf("%s != %s", b, test.wanted)
		}
	}
}
