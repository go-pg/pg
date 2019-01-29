package orm

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type User struct {
	tableName struct{} `sql:"user"`
}

type User2 struct {
	tableName struct{} `sql:"select:user,alias:user"`
}

type SelectModel struct {
	Id       int
	Name     string
	HasOne   *HasOneModel
	HasOneId int
	HasMany  []HasManyModel
}

type HasOneModel struct {
	Id int
}

type HasManyModel struct {
	Id            int
	SelectModelId int
}

var _ = Describe("Select", func() {
	It("works with User model", func() {
		q := NewQuery(nil, &User{})

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT  FROM "user" AS "user"`))
	})

	It("works with User model", func() {
		q := NewQuery(nil, &User2{})

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT  FROM "user" AS "user"`))
	})

	It("copies query", func() {
		q1 := NewQuery(nil).Where("1 = 1").Where("2 = 2").Where("3 = 3")
		q2 := q1.Copy().Where("q2 = ?", "v2")
		_ = q1.Where("q1 = ?", "v1")

		b, err := (&selectQuery{q: q2}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal("SELECT * WHERE (1 = 1) AND (2 = 2) AND (3 = 3) AND (q2 = 'v2')"))
	})

	It("specifies all columns", func() {
		q := NewQuery(nil, &SelectModel{})

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model"`))
	})

	It("omits columns in main query", func() {
		q := NewQuery(nil, &SelectModel{}).Column("_").Relation("HasOne")

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "has_one"."id" AS "has_one__id" FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id"`))
	})

	It("adds JoinOn", func() {
		q := NewQuery(nil, &SelectModel{}).
			Relation("HasOne", func(q *Query) (*Query, error) {
				q = q.JoinOn("1 = 2")
				return q, nil
			})

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id", "has_one"."id" AS "has_one__id" FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id" AND (1 = 2)`))
	})

	It("omits columns in join query", func() {
		q := NewQuery(nil, &SelectModel{}).Relation("HasOne._")

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id"`))
	})

	It("specifies all columns for has one", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).Relation("HasOne")

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id", "has_one"."id" AS "has_one__id" FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id"`))
	})

	It("specifies all columns for has many", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).Relation("HasMany")

		q, err := q.model.GetJoin("HasMany").manyQuery(q.New())
		Expect(err).NotTo(HaveOccurred())

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "has_many_model"."id", "has_many_model"."select_model_id" FROM "has_many_models" AS "has_many_model" WHERE ("has_many_model"."select_model_id" IN (1))`))
	})

	It("overwrites columns for has many", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).
			Relation("HasMany", func(q *Query) (*Query, error) {
				q = q.ColumnExpr("expr")
				return q, nil
			})

		q, err := q.model.GetJoin("HasMany").manyQuery(q.New())
		Expect(err).NotTo(HaveOccurred())

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT expr FROM "has_many_models" AS "has_many_model" WHERE ("has_many_model"."select_model_id" IN (1))`))
	})

	It("expands ?TableColumns", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).ColumnExpr("?TableColumns")

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model"`))
	})

	It("expands ?Columns", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).ColumnExpr("?Columns")

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "id", "name", "has_one_id" FROM "select_models" AS "select_model"`))
	})

	It("supports multiple groups", func() {
		q := NewQuery(nil).Group("one").Group("two")
		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT * GROUP BY "one", "two"`))
	})

	It("WhereOr", func() {
		q := NewQuery(nil).Where("1 = 1").WhereOr("1 = 2")

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT * WHERE (1 = 1) OR (1 = 2)`))
	})

	It("supports subqueries", func() {
		subq := NewQuery(nil, &SelectModel{}).Column("id").Where("name IS NOT NULL")
		q := NewQuery(nil).Where("id IN (?)", subq)

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT * WHERE (id IN (SELECT "id" FROM "select_models" AS "select_model" WHERE (name IS NOT NULL)))`))
	})

	It("supports locking", func() {
		q := NewQuery(nil).For("UPDATE SKIP LOCKED")
		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT * FOR UPDATE SKIP LOCKED`))
	})

	It("supports WhereGroup", func() {
		q := NewQuery(nil).Where("TRUE").WhereGroup(func(q *Query) (*Query, error) {
			q = q.Where("FALSE").WhereOr("TRUE")
			return q, nil
		})
		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT * WHERE (TRUE) AND ((FALSE) OR (TRUE))`))
	})

	It("supports WhereOrGroup", func() {
		q := NewQuery(nil).Where("TRUE").WhereOrGroup(func(q *Query) (*Query, error) {
			q = q.Where("FALSE").Where("TRUE")
			return q, nil
		})
		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT * WHERE (TRUE) OR ((FALSE) AND (TRUE))`))
	})

	It("supports empty WhereGroup", func() {
		q := NewQuery(nil).Where("TRUE").WhereGroup(func(q *Query) (*Query, error) {
			return q, nil
		})
		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT * WHERE (TRUE)`))
	})

	It("expands ?TableAlias in Where with structs", func() {
		t := time.Date(2006, 2, 3, 10, 30, 35, 987654321, time.UTC)
		q := NewQuery(nil, &SelectModel{}).Column("id").Where("?TableAlias.name > ?", t)

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "id" FROM "select_models" AS "select_model" WHERE ("select_model".name > '2006-02-03 10:30:35.987654321+00:00:00')`))
	})
})

var _ = Describe("Count", func() {
	It("removes LIMIT, OFFSET, and ORDER", func() {
		q := NewQuery(nil).Order("order").Limit(1).Offset(2)

		b, err := q.countSelectQuery("count(*)").AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT count(*)`))
	})

	It("does not remove LIMIT, OFFSET, and ORDER from CTE", func() {
		q := NewQuery(nil).
			Column("col1", "col2").
			Order("order").
			Limit(1).
			Offset(2).
			WrapWith("wrapper").
			Table("wrapper").
			Order("order").
			Limit(1).
			Offset(2)

		b, err := q.countSelectQuery("count(*)").AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`WITH "wrapper" AS (SELECT "col1", "col2" ORDER BY "order" LIMIT 1 OFFSET 2) SELECT count(*) FROM "wrapper"`))
	})

	It("includes has one joins", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).Relation("HasOne")

		b, err := q.countSelectQuery("count(*)").AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT count(*) FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id"`))
	})

	It("uses CTE when query contains GROUP BY", func() {
		q := NewQuery(nil).Group("one")

		b, err := q.countSelectQuery("count(*)").AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`WITH "_count_wrapper" AS (SELECT * GROUP BY "one") SELECT count(*) FROM "_count_wrapper"`))
	})

	It("uses CTE when column contains DISTINCT", func() {
		q := NewQuery(nil).ColumnExpr("DISTINCT group_id")

		b, err := q.countSelectQuery("count(*)").AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`WITH "_count_wrapper" AS (SELECT DISTINCT group_id) SELECT count(*) FROM "_count_wrapper"`))
	})
})

var _ = Describe("With", func() {
	It("WrapWith wraps query in CTE", func() {
		q := NewQuery(nil, &SelectModel{}).
			Where("cond1").
			WrapWith("wrapper").
			Table("wrapper").
			Where("cond2")

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`WITH "wrapper" AS (SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model" WHERE (cond1)) SELECT * FROM "wrapper" WHERE (cond2)`))
	})

	It("generates nested CTE", func() {
		q1 := NewQuery(nil).Table("q1")
		q2 := NewQuery(nil).With("q1", q1).Table("q2", "q1")
		q3 := NewQuery(nil).With("q2", q2).Table("q3", "q2")

		b, err := (&selectQuery{q: q3}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`WITH "q2" AS (WITH "q1" AS (SELECT * FROM "q1") SELECT * FROM "q2", "q1") SELECT * FROM "q3", "q2"`))
	})

	It("supports Join.JoinOn.JoinOnOr", func() {
		q := NewQuery(nil).Table("t1").
			Join("JOIN t2").JoinOn("t2.c1 = t1.c1").JoinOn("t2.c2 = t1.c1").
			Join("JOIN t3").JoinOn("t3.c1 = t3.c2").JoinOnOr("t3.c2 = t1.c2")

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT * FROM "t1" JOIN t2 ON (t2.c1 = t1.c1) AND (t2.c2 = t1.c1) JOIN t3 ON (t3.c1 = t3.c2) OR (t3.c2 = t1.c2)`))
	})

	It("excludes a column", func() {
		q := NewQuery(nil, &SelectModel{}).
			ExcludeColumn("has_one_id")

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "id", "name" FROM "select_models" AS "select_model"`))
	})
})

type orderTest struct {
	order string
	query string
}

var _ = Describe("Select Order", func() {
	orderTests := []orderTest{
		{"id", `"id"`},
		{"id asc", `"id" asc`},
		{"id desc", `"id" desc`},
		{"id ASC", `"id" ASC`},
		{"id DESC", `"id" DESC`},
		{"id ASC NULLS FIRST", `"id" ASC NULLS FIRST`},
	}

	It("sets order", func() {
		for _, test := range orderTests {
			q := NewQuery(nil).Order(test.order)

			b, err := (&selectQuery{q: q}).AppendQuery(nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(Equal(`SELECT * ORDER BY ` + test.query))
		}
	})
})

type SoftDeleteModel struct {
	Id        int
	DeletedAt time.Time `pg:",soft_delete"`
}

var _ = Describe("SoftDeleteModel", func() {
	It("works with User model", func() {
		q := NewQuery(nil, &SoftDeleteModel{})

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "soft_delete_model"."id", "soft_delete_model"."deleted_at" FROM "soft_delete_models" AS "soft_delete_model" WHERE "soft_delete_model"."deleted_at" IS NULL`))
	})
})
