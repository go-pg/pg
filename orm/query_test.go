package orm_test

import (
	"testing"
	"unsafe"

	"gopkg.in/pg.v4/orm"
)

func TestQuerySize(t *testing.T) {
	size := int(unsafe.Sizeof(orm.Query{}))
	wanted := 344
	if size != wanted {
		t.Fatalf("got %d, wanted %d", size, wanted)
	}
}
