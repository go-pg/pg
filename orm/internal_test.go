package orm

import (
	"reflect"
	"sync"
	"testing"
)

type TableInitRace struct {
	Id  int
	Foo int

	HasSelf   *TableInitRace
	HasSelfId int

	HasOne1   *TableInitRace1
	HasOne1Id int

	HasAnother1   *TableInitRace1
	HasAnother1Id int

	Bar int
}

type TableInitRace1 struct {
	Id  int
	Foo int

	HasOne2   *TableInitRace2
	HasOne2Id int

	Bar int
}

type TableInitRace2 struct {
	Id  int
	Foo int

	HasOne3   *TableInitRace3
	HasOne3Id int

	Bar int
}

type TableInitRace3 struct {
	Id  int
	Foo int
	Bar int
}

func TestTableInitRace(t *testing.T) {
	const C = 16

	types := []reflect.Type{
		reflect.TypeOf((*TableInitRace)(nil)).Elem(),
		reflect.TypeOf((*TableInitRace1)(nil)).Elem(),
		reflect.TypeOf((*TableInitRace2)(nil)).Elem(),
		reflect.TypeOf((*TableInitRace3)(nil)).Elem(),
	}

	var wg sync.WaitGroup
	for _, typ := range types {
		wg.Add(C)
		for i := 0; i < C; i++ {
			go func(typ reflect.Type, i int) {
				if i%2 == 0 {
					_ = _tables.get(typ, true)
				} else {
					_ = _tables.get(typ, false)
				}
				wg.Done()
			}(typ, i)
		}
	}
	wg.Wait()
}
