package orm

import (
	"github.com/go-pg/pg/v9/types"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type InsertTest struct {
	Id    int
	Value string
}

type EmbeddingTest struct {
	tableName struct{} `sql:"name"`

	Id    int
	Field int
}

type EmbeddedInsertTest struct {
	tableName struct{} `sql:"my_name"`
	EmbeddingTest
	Field2 int
}

type InheritInsertTest struct {
	EmbeddingTest `pg:",inherit"`
	Field2        int
}

type InsertNullTest struct {
	F1 int
	F2 int `pg:",usezero"`
	F3 int `sql:",pk"`
	F4 int `sql:",pk" pg:",usezero"`
}

type InsertDefaultTest struct {
	Id    int
	Value string `sql:"default:hello"`
}

type InsertQTest struct {
	Geo  types.Q
	Func types.ValueAppender
}

var _ = Describe("Insert", func() {
	It("supports Column", func() {
		model := &InsertTest{
			Id:    1,
			Value: "hello",
		}
		q := NewQuery(nil, model).Column("id")

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO "insert_tests" ("id") VALUES (1)`))
	})

	It("supports Value", func() {
		model := &InsertTest{
			Id:    1,
			Value: "hello",
		}
		q := NewQuery(nil, model).Value("value", "upper(?)", model.Value)

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO "insert_tests" ("id", "value") VALUES (1, upper('hello'))`))
	})

	It("supports Value 2", func() {
		model := &InsertTest{
			Id:    1,
			Value: "hello",
		}
		q := NewQuery(nil, model).Value("value", "upper(?value)")

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO "insert_tests" ("id", "value") VALUES (1, upper('hello'))`))
	})

	It("supports extra Value", func() {
		model := &InsertTest{
			Id:    1,
			Value: "hello",
		}
		q := NewQuery(nil, model).Value("unknown", "upper(?)", model.Value)

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO "insert_tests" ("id", "value", "unknown") VALUES (1, 'hello', upper('hello'))`))
	})

	It("multi inserts", func() {
		q := NewQuery(nil, &InsertTest{
			Id:    1,
			Value: "hello",
		}, &InsertTest{
			Id: 2,
		})

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO "insert_tests" ("id", "value") VALUES (1, 'hello'), (2, DEFAULT) RETURNING "value"`))
	})

	It("supports ON CONFLICT DO UPDATE", func() {
		q := NewQuery(nil, &InsertTest{}).
			Where("1 = 1").
			OnConflict("(unq1) DO UPDATE").
			Set("count1 = count1 + 1").
			Where("2 = 2")

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO "insert_tests" AS "insert_test" ("id", "value") VALUES (DEFAULT, DEFAULT) ON CONFLICT (unq1) DO UPDATE SET count1 = count1 + 1 WHERE (2 = 2) RETURNING "id", "value"`))
	})

	It("supports ON CONFLICT DO UPDATE without SET", func() {
		q := NewQuery(nil, &InsertTest{}).
			OnConflict("(unq1) DO UPDATE")

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO "insert_tests" AS "insert_test" ("id", "value") VALUES (DEFAULT, DEFAULT) ON CONFLICT (unq1) DO UPDATE SET "value" = EXCLUDED."value" RETURNING "id", "value"`))
	})

	It("supports ON CONFLICT DO NOTHING", func() {
		q := NewQuery(nil, &InsertTest{}).
			OnConflict("(unq1) DO NOTHING").
			Set("count1 = count1 + 1").
			Where("cond1 IS TRUE")

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO "insert_tests" AS "insert_test" ("id", "value") VALUES (DEFAULT, DEFAULT) ON CONFLICT (unq1) DO NOTHING RETURNING "id", "value"`))
	})

	It("supports custom table name on embedded struct", func() {
		q := NewQuery(nil, &EmbeddedInsertTest{})

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO my_name ("id", "field", "field2") VALUES (DEFAULT, DEFAULT, DEFAULT) RETURNING "id", "field", "field2"`))
	})

	It("inherits table name from embedded struct", func() {
		q := NewQuery(nil, &InheritInsertTest{})

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO name ("id", "field", "field2") VALUES (DEFAULT, DEFAULT, DEFAULT) RETURNING "id", "field", "field2"`))
	})

	It("supports value when default value is set", func() {
		q := NewQuery(nil, &InsertDefaultTest{Id: 1})

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO "insert_default_tests" ("id", "value") VALUES (1, DEFAULT) RETURNING "value"`))
	})

	It("supports RETURNING NULL", func() {
		q := NewQuery(nil, &InsertDefaultTest{Id: 1}).Returning("NULL")

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO "insert_default_tests" ("id", "value") VALUES (1, DEFAULT)`))
	})

	It("supports usezero tag", func() {
		q := NewQuery(nil, &InsertNullTest{})

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO "insert_null_tests" ("f1", "f2", "f3", "f4") VALUES (DEFAULT, 0, DEFAULT, 0) RETURNING "f1", "f3"`))
	})

	It("inserts types.Q", func() {
		q := NewQuery(nil, &InsertQTest{
			Geo:  types.Q("ST_GeomFromText('POLYGON((75.150000 29.530000, 77.000000 29.000000, 77.600000 29.500000, 75.150000 29.530000))')"),
			Func: Q("my_func(?)", "param"),
		})

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO "insert_q_tests" ("geo", "func") VALUES (ST_GeomFromText('POLYGON((75.150000 29.530000, 77.000000 29.000000, 77.600000 29.500000, 75.150000 29.530000))'), my_func('param'))`))
	})

	It("supports FROM", func() {
		q := NewQuery(nil, (*InsertTest)(nil))
		q = q.WrapWith("data").
			TableExpr("dst").
			ColumnExpr("dst_col1, dst_col2").
			TableExpr("data")

		s := insertQueryString(q)
		Expect(s).To(Equal(`WITH "data" AS (SELECT "insert_test"."id", "insert_test"."value" FROM "insert_tests" AS "insert_test") INSERT INTO dst (dst_col1, dst_col2) SELECT * FROM data`))
	})

	It("bulk inserts", func() {
		q := NewQuery(nil, &InsertTest{}, &InsertTest{})

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO "insert_tests" ("id", "value") VALUES (DEFAULT, DEFAULT), (DEFAULT, DEFAULT) RETURNING "id", "value"`))
	})

	It("bulk inserts overriding column value", func() {
		q := NewQuery(nil, &InsertTest{}, &InsertTest{}).
			Value("id", "123")

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO "insert_tests" ("id", "value") VALUES (123, DEFAULT), (123, DEFAULT) RETURNING "value"`))
	})

	It("returns an error for empty bulk insert", func() {
		slice := make([]InsertTest, 0)
		q := NewQuery(nil, &slice)

		_, err := (&insertQuery{q: q}).AppendQuery(defaultFmter, nil)
		Expect(err).To(MatchError("pg: can't bulk-insert empty slice []orm.InsertTest"))
	})

	It("supports notnull and default", func() {
		type Model struct {
			Id   int
			Bool bool `sql:",default:_"`
		}

		q := NewQuery(nil, &Model{})

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO "models" ("id", "bool") VALUES (DEFAULT, DEFAULT) RETURNING "id", "bool"`))
	})

	It("support models without a name", func() {
		type Model struct {
			tableName struct{} `sql:"_"`
			Id        int
		}

		q := NewQuery(nil, &Model{}).Table("dynamic_name")

		s := insertQueryString(q)
		Expect(s).To(Equal(`INSERT INTO "dynamic_name" ("id") VALUES (DEFAULT) RETURNING "id"`))
	})
})

func insertQueryString(q *Query) string {
	ins := newInsertQuery(q)
	return queryString(ins)
}
