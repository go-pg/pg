package orm_test

import (
	"reflect"
	"sync"
	"testing"

	"github.com/go-pg/pg/orm"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type A struct {
	Id int
}

func (A) Method() int {
	return 10
}

type B struct {
	A
}

var _ = Describe("embedded Model", func() {
	var strct reflect.Value
	var table *orm.Table

	BeforeEach(func() {
		strct = reflect.ValueOf(B{A: A{Id: 1}})
		table = orm.GetTable(strct.Type())
	})

	It("has fields", func() {
		Expect(table.Fields).To(HaveLen(1))
		Expect(table.FieldsMap).To(HaveLen(1))

		id, ok := table.FieldsMap["id"]
		Expect(ok).To(BeTrue())
		Expect(id.GoName).To(Equal("Id"))
		Expect(id.SQLName).To(Equal("id"))
		Expect(string(id.Column)).To(Equal(`"id"`))
		Expect(id.HasFlag(orm.PrimaryKeyFlag)).To(BeTrue())
		Expect(string(id.AppendValue(nil, strct, 1))).To(Equal("1"))

		Expect(table.PKs).To(HaveLen(1))
		Expect(table.PKs[0]).To(Equal(id))
	})

	It("has methods", func() {
		Expect(table.Methods).To(HaveLen(1))

		m, ok := table.Methods["Method"]
		Expect(ok).To(BeTrue())
		Expect(m.Index).To(Equal(0))
		Expect(string(m.AppendValue(nil, strct, 1))).To(Equal("10"))
	})
})

type C struct {
	Name int `sql:",pk"`
	Id   int
	UUID int
}

var _ = Describe("primary key annotation", func() {
	var table *orm.Table

	BeforeEach(func() {
		strct := reflect.ValueOf(C{})
		table = orm.GetTable(strct.Type())
	})

	It("has precedence over auto-detection", func() {
		Expect(table.PKs).To(HaveLen(1))
		Expect(table.PKs[0].GoName).To(Equal("Name"))
	})
})

type D struct {
	UUID int
}

var _ = Describe("uuid field", func() {
	var table *orm.Table

	BeforeEach(func() {
		strct := reflect.ValueOf(D{})
		table = orm.GetTable(strct.Type())
	})

	It("is detected as primary key", func() {
		Expect(table.PKs).To(HaveLen(1))
		Expect(table.PKs[0].GoName).To(Equal("UUID"))
	})
})

type E struct {
	Id          int
	StructField struct {
		Foo string
		Bar string
	}
}

var _ = Describe("struct field", func() {
	var table *orm.Table

	BeforeEach(func() {
		strct := reflect.ValueOf(E{})
		table = orm.GetTable(strct.Type())
	})

	It("is present in the list", func() {
		Expect(table.Fields).To(HaveLen(2))

		_, ok := table.FieldsMap["struct_field"]
		Expect(ok).To(BeTrue())
	})
})

type f struct {
	Id int
	G  *g
}

type g struct {
	Id  int
	FId int
	F   *f
}

var _ = Describe("unexported types", func() {
	It("work with belongs to relation", func() {
		strct := reflect.ValueOf(f{})
		table := orm.GetTable(strct.Type())

		rel, ok := table.Relations["G"]
		Expect(ok).To(BeTrue())
		Expect(rel.Type).To(Equal(orm.BelongsToRelation))
	})

	It("work with has one relation", func() {
		strct := reflect.ValueOf(g{})
		table := orm.GetTable(strct.Type())

		rel, ok := table.Relations["F"]
		Expect(ok).To(BeTrue())
		Expect(rel.Type).To(Equal(orm.HasOneRelation))
	})
})

type H struct {
	I *I
}

type I struct {
	H *H
}

var _ = Describe("model with circular reference", func() {
	It("works", func() {
		table := orm.GetTable(reflect.TypeOf(H{}))
		Expect(table).NotTo(BeNil())

		table = orm.GetTable(reflect.TypeOf(I{}))
		Expect(table).NotTo(BeNil())
	})
})

type J struct {
	JId int64 `sql:",pk"`
}

type K struct {
	KId  int64 `sql:",pk"`
	MyId int64
	My   *J
}

var _ = Describe("ModelId fk", func() {
	It("is autodetected", func() {
		table := orm.GetTable(reflect.TypeOf(K{}))
		Expect(table).NotTo(BeNil())

		rel := table.Relations["My"]
		Expect(rel).NotTo(BeNil())
	})
})

type L struct {
	Id int64
}

var _ = Describe("ModelId fk and anonymous model", func() {
	It("is autodetected", func() {
		var res struct {
			Items []L
		}

		table := orm.GetTable(reflect.TypeOf(res))
		Expect(table).NotTo(BeNil())

		field := table.FieldsMap["items"]
		Expect(field).NotTo(BeNil())

		rel := table.Relations["Items"]
		Expect(rel).To(BeNil())
	})
})

type M struct {
	Id   int64
	Name string
}

type N struct {
	M
	Id string
}

var _ = Describe("embedding", func() {
	It("handles overwriting", func() {
		table := orm.GetTable(reflect.TypeOf(N{}))
		Expect(table.Fields).To(HaveLen(2))
		Expect(table.FieldsMap).To(HaveLen(2))
		Expect(table.PKs).To(HaveLen(1))
		Expect(table.DataFields).To(HaveLen(1))

		field := table.FieldsMap["id"]
		Expect(field.SQLType).To(Equal("text"))
	})
})

type TableInitRace struct {
	HasOne1Id int
	HasOne1   *TableInitRace1

	HasOne2Id int
	HasOne2   *TableInitRace2

	HasOne3Id int
	HasOne3   *TableInitRace3
}

type TableInitRace1 struct {
	Id int
}

type TableInitRace2 struct {
	Id int
}

type TableInitRace3 struct {
	Id int
}

func TestTableInitRace(t *testing.T) {
	const C = 10

	typ := reflect.TypeOf((*TableInitRace)(nil)).Elem()

	var wg sync.WaitGroup
	wg.Add(C)
	for i := 0; i < C; i++ {
		go func() {
			_ = orm.GetTable(typ)
			wg.Done()
		}()
	}
	wg.Wait()
}
