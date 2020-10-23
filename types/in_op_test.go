package types_test

import (
	"testing"

	"github.com/go-pg/pg/v11/types"
	"github.com/stretchr/testify/assert"
)

func TestInOp(t *testing.T) {
	_, err := types.In(&[]string{}).AppendValue(nil, 0)
	assert.EqualError(t, err, "pg: In(non-slice *[]string)")

	tests := []struct {
		app    types.ValueAppender
		wanted string
	}{
		{types.In([]int{}), ""},
		{types.In([]int{1}), "1"},
		{types.In([]int{1, 2, 3}), "1,2,3"},
		{types.In([][]int{{1, 2, 3}}), "(1,2,3)"},
		{types.In([][]int{{1, 2}, []int{3, 4}}), "(1,2),(3,4)"},
		{types.In([]*types.Array{
			types.NewArray([]int{1, 2}),
			types.NewArray([]int{3, 4}),
		}), "{1,2},{3,4}"},
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
