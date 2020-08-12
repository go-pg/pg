package orm

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Select - nil model", func() {
	It("works with User model", func() {
		q := NewQuery(nil, (*User)(nil))

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT  FROM "user" AS "user"`))
	})

	It("works with User model", func() {
		q := NewQuery(nil, (*User2)(nil))

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT  FROM "user" AS "user"`))
	})

	It("specifies all columns", func() {
		q := NewQuery(nil, (*SelectModel)(nil))

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model"`))
	})

	It("omits columns in main query", func() {
		q := NewQuery(nil, (*SelectModel)(nil)).Column("_").Relation("HasOne")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "has_one"."id" AS "has_one__id" FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id"`))
	})

	It("adds JoinOn", func() {
		q := NewQuery(nil, (*SelectModel)(nil)).
			Relation("HasOne", func(q *Query) (*Query, error) {
				q = q.JoinOn("1 = 2")
				return q, nil
			})

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id", "has_one"."id" AS "has_one__id" FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id" AND (1 = 2)`))
	})

	It("omits columns in join query", func() {
		q := NewQuery(nil, (*SelectModel)(nil)).Relation("HasOne._")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id"`))
	})

	It("supports subqueries", func() {
		subq := NewQuery(nil, (*SelectModel)(nil)).Column("id").Where("name IS NOT NULL")
		q := NewQuery(nil).Where("id IN (?)", subq)

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT * WHERE (id IN (SELECT "id" FROM "select_models" AS "select_model" WHERE (name IS NOT NULL)))`))
	})

	It("expands ?TableAlias in Where with structs", func() {
		t := time.Date(2006, 2, 3, 10, 30, 35, 987654321, time.UTC)
		q := NewQuery(nil, (*SelectModel)(nil)).Column("id").Where("?TableAlias.name > ?", t)

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "id" FROM "select_models" AS "select_model" WHERE ("select_model".name > '2006-02-03 10:30:35.987654321+00:00:00')`))
	})

	It("supports DISTINCT", func() {
		q := NewQuery(nil, (*SelectModel)(nil)).Distinct()

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT DISTINCT "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model"`))
	})

	It("supports DISTINCT ON", func() {
		q := NewQuery(nil, (*SelectModel)(nil)).DistinctOn("expr(?)", "foo")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT DISTINCT ON (expr('foo')) "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model"`))
	})
})

var _ = Describe("With - nil model", func() {
	It("WrapWith wraps query in CTE", func() {
		q := NewQuery(nil, (*SelectModel)(nil)).
			Where("cond1").
			WrapWith("wrapper").
			Table("wrapper").
			Where("cond2")

		s := selectQueryString(q)
		Expect(s).To(Equal(`WITH "wrapper" AS (SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model" WHERE (cond1)) SELECT * FROM "wrapper" WHERE (cond2)`))
	})

	It("excludes a column", func() {
		q := NewQuery(nil, (*SelectModel)(nil)).
			ExcludeColumn("has_one_id")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "id", "name" FROM "select_models" AS "select_model"`))
	})

	It("supports WithDelete", func() {
		subq := NewQuery(nil, (*SelectModel)(nil)).
			Where("cond1")

		q := NewQuery(nil).
			WithDelete("wrapper", subq).
			Table("wrapper").
			Where("cond2")

		s := selectQueryString(q)
		Expect(s).To(Equal(`WITH "wrapper" AS (DELETE FROM "select_models" AS "select_model" WHERE (cond1)) SELECT * FROM "wrapper" WHERE (cond2)`))
	})
})

var _ = Describe("SoftDeleteModel - nil model", func() {
	It("filters out deleted rows by default", func() {
		q := NewQuery(nil, (*SoftDeleteModel)(nil))

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "soft_delete_model"."id", "soft_delete_model"."deleted_at" FROM "soft_delete_models" AS "soft_delete_model" WHERE "soft_delete_model"."deleted_at" IS NULL`))
	})

	It("supports Deleted", func() {
		q := NewQuery(nil, (*SoftDeleteModel)(nil)).Deleted()

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "soft_delete_model"."id", "soft_delete_model"."deleted_at" FROM "soft_delete_models" AS "soft_delete_model" WHERE "soft_delete_model"."deleted_at" IS NOT NULL`))
	})

	It("supports AllWithDeleted", func() {
		q := NewQuery(nil, (*SoftDeleteModel)(nil)).AllWithDeleted()

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "soft_delete_model"."id", "soft_delete_model"."deleted_at" FROM "soft_delete_models" AS "soft_delete_model"`))
	})

	It("will respect join SoftDelete", func() {
		q := NewQuery(nil, (*SoftDeleteParent)(nil)).Relation("Children").Relation("Children.SubChildren")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "soft_delete_parent"."id", "soft_delete_parent"."name", "soft_delete_parent"."date_deleted", "children"."id" AS "children__id", "children"."soft_delete_parent_id" AS "children__soft_delete_parent_id", "children"."name" AS "children__name", "children__sub_children"."id" AS "children__sub_children__id", "children__sub_children"."soft_delete_child_id" AS "children__sub_children__soft_delete_child_id", "children__sub_children"."name" AS "children__sub_children__name" FROM "soft_delete_parents" AS "soft_delete_parent" LEFT JOIN "soft_delete_children" AS "children" ON "children"."soft_delete_parent_id" = "soft_delete_parent"."id" LEFT JOIN "soft_delete_sub_children" AS "children__sub_children" ON "children__sub_children"."soft_delete_child_id" = "children"."id" WHERE "soft_delete_parent"."date_deleted" IS NULL`))
	})

	It("will join a non-SoftDelete with a SoftDelete", func() {
		q := NewQuery(nil, (*NonSoftDeleteModel)(nil)).Relation("SoftDeleteModel")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "non_soft_delete_model"."id", "non_soft_delete_model"."name", "non_soft_delete_model"."soft_delete_model_id", "soft_delete_model"."id" AS "soft_delete_model__id", "soft_delete_model"."deleted_at" AS "soft_delete_model__deleted_at" FROM "non_soft_delete_models" AS "non_soft_delete_model" LEFT JOIN "soft_delete_models" AS "soft_delete_model" ON ("soft_delete_model"."id" = "non_soft_delete_model"."soft_delete_model_id") AND "soft_delete_model"."deleted_at" IS NULL`))
	})
})
