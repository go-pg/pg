package internal_test

import (
	"testing"

	"github.com/go-pg/pg/internal"
)

func TestUnderscore(t *testing.T) {
	tests := []struct {
		s, wanted string
	}{
		{"Megacolumn", "megacolumn"},
		{"MegaColumn", "mega_column"},
		{"MegaColumn_Id", "mega_column__id"},
		{"MegaColumn_id", "mega_column_id"},
	}
	for _, v := range tests {
		if got := internal.Underscore(v.s); got != v.wanted {
			t.Errorf("got %q, wanted %q", got, v.wanted)
		}
	}
}
