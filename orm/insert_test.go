package orm

import (
	"gopkg.in/pg.v5/types"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type InsertTest struct{}

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

type OverrideInsertTest struct {
	EmbeddingTest `pg:",override"`
	Field2        int
}

type InsertNullTest struct {
	F1 int
	F2 int `sql:",notnull"`
	F3 int `sql:",pk"`
	F4 int `sql:",pk,notnull"`
}

type InsertQTest struct {
	Geo types.Q
}

var _ = Describe("Insert", func() {
	It("supports ON CONFLICT DO UPDATE", func() {
		q := NewQuery(nil, &InsertTest{}).
			OnConflict("(unq1) DO UPDATE").
			Set("count1 = count1 + 1").
			Where("cond1 IS TRUE")

		b, err := insertQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`INSERT INTO "insert_tests" AS "insert_test" () VALUES () ON CONFLICT (unq1) DO UPDATE SET count1 = count1 + 1 WHERE (cond1 IS TRUE)`))
	})

	It("supports ON CONFLICT DO NOTHING", func() {
		q := NewQuery(nil, &InsertTest{}).
			OnConflict("(unq1) DO NOTHING").
			Set("count1 = count1 + 1").
			Where("cond1 IS TRUE")

		b, err := insertQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`INSERT INTO "insert_tests" AS "insert_test" () VALUES () ON CONFLICT (unq1) DO NOTHING`))
	})

	It("supports custom table name on embedded struct", func() {
		q := NewQuery(nil, &EmbeddedInsertTest{})

		b, err := insertQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`INSERT INTO my_name ("id", "field", "field2") VALUES (DEFAULT, DEFAULT, DEFAULT) RETURNING "id", "field", "field2"`))
	})

	It("supports override table name with embedded struct", func() {
		q := NewQuery(nil, &OverrideInsertTest{})

		b, err := insertQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`INSERT INTO name ("id", "field", "field2") VALUES (DEFAULT, DEFAULT, DEFAULT) RETURNING "id", "field", "field2"`))
	})

	It("supports notnull", func() {
		q := NewQuery(nil, &InsertNullTest{})

		b, err := insertQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`INSERT INTO "insert_null_tests" ("f1", "f2", "f3", "f4") VALUES (DEFAULT, 0, DEFAULT, 0) RETURNING "f1", "f3"`))
	})

	It("inserts types.Q", func() {
		q := NewQuery(nil, &InsertQTest{
			Geo: types.Q("ST_GeomFromText('POLYGON((75.150000 29.530000, 77.000000 29.000000, 77.600000 29.500000, 75.150000 29.530000))')"),
		})

		b, err := insertQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`INSERT INTO "insert_q_tests" ("geo") VALUES (ST_GeomFromText('POLYGON((75.150000 29.530000, 77.000000 29.000000, 77.600000 29.500000, 75.150000 29.530000))'))`))
	})
})
