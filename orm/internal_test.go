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

type TableInlineRace struct {
	Id  int
	Foo int

	R1   *TableInlineRace1
	R1Id int
	R10  *TableInlineRace1

	R2   *TableInlineRace2
	R2Id int
	R20  *TableInlineRace2

	R3   *TableInitRace3
	R3Id int
	R30  *TableInitRace3
}

type TableInlineRace1 struct {
	Id   int
	Foo1 int

	R2   *TableInlineRace2
	R2Id int
	R20  *TableInlineRace2

	R3   *TableInitRace3
	R3Id int
	R30  *TableInitRace3
}

type TableInlineRace2 struct {
	Id   int
	Foo2 int

	R3   *TableInlineRace3
	R3Id int
	R30  *TableInlineRace3
}

type TableInlineRace3 struct {
	Id   int
	Foo2 int
}

func TestTableInlineRace(t *testing.T) {
	const C = 32

	types := []reflect.Type{
		reflect.TypeOf((*TableInlineRace)(nil)).Elem(),
		reflect.TypeOf((*TableInlineRace1)(nil)).Elem(),
		reflect.TypeOf((*TableInlineRace2)(nil)).Elem(),
		reflect.TypeOf((*TableInlineRace3)(nil)).Elem(),
	}

	var wg sync.WaitGroup
	for _, typ := range types {
		wg.Add(C)
		for i := 0; i < C; i++ {
			go func(typ reflect.Type, i int) {
				_ = _tables.get(typ, false)
				wg.Done()
			}(typ, i)
		}
	}
	wg.Wait()
}
