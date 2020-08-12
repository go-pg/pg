package orm

import (
	"time"

	"github.com/go-pg/pg/v10/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type User struct {
	tableName struct{} `pg:"user"`
}

type User2 struct {
	tableName struct{} `pg:"select:user,alias:user"`
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

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT  FROM "user" AS "user"`))
	})

	It("works with User model", func() {
		q := NewQuery(nil, &User2{})

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT  FROM "user" AS "user"`))
	})

	It("copies query", func() {
		q1 := NewQuery(nil).Where("1 = 1").Where("2 = 2").Where("3 = 3")
		q2 := q1.Clone().Where("q2 = ?", "v2")
		_ = q1.Where("q1 = ?", "v1")

		s := selectQueryString(q2)
		Expect(s).To(Equal("SELECT * WHERE (1 = 1) AND (2 = 2) AND (3 = 3) AND (q2 = 'v2')"))
	})

	It("specifies all columns", func() {
		q := NewQuery(nil, &SelectModel{})

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model"`))
	})

	It("omits columns in main query", func() {
		q := NewQuery(nil, &SelectModel{}).Column("_").Relation("HasOne")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "has_one"."id" AS "has_one__id" FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id"`))
	})

	It("adds JoinOn", func() {
		q := NewQuery(nil, &SelectModel{}).
			Relation("HasOne", func(q *Query) (*Query, error) {
				q = q.JoinOn("1 = 2")
				return q, nil
			})

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id", "has_one"."id" AS "has_one__id" FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id" AND (1 = 2)`))
	})

	It("omits columns in join query", func() {
		q := NewQuery(nil, &SelectModel{}).Relation("HasOne._")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id"`))
	})

	It("specifies all columns for has one", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).Relation("HasOne")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id", "has_one"."id" AS "has_one__id" FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id"`))
	})

	It("specifies all columns for has many", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).Relation("HasMany")

		q, err := q.tableModel.GetJoin("HasMany").manyQuery(q.New())
		Expect(err).NotTo(HaveOccurred())

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "has_many_model"."id", "has_many_model"."select_model_id" FROM "has_many_models" AS "has_many_model" WHERE ("has_many_model"."select_model_id" IN (1))`))
	})

	It("overwrites columns for has many", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).
			Relation("HasMany", func(q *Query) (*Query, error) {
				q = q.ColumnExpr("expr")
				return q, nil
			})

		q, err := q.tableModel.GetJoin("HasMany").manyQuery(q.New())
		Expect(err).NotTo(HaveOccurred())

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT expr FROM "has_many_models" AS "has_many_model" WHERE ("has_many_model"."select_model_id" IN (1))`))
	})

	It("expands ?TableColumns", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).ColumnExpr("?TableColumns")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model"`))
	})

	It("expands ?Columns", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).ColumnExpr("?Columns")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "id", "name", "has_one_id" FROM "select_models" AS "select_model"`))
	})

	It("expands ?TablePKs", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).ColumnExpr("?TablePKs")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "select_model"."id" FROM "select_models" AS "select_model"`))
	})

	It("expands ?PKs", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).ColumnExpr("?PKs")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "id" FROM "select_models" AS "select_model"`))
	})

	It("supports multiple groups", func() {
		q := NewQuery(nil).Group("one").Group("two")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT * GROUP BY "one", "two"`))
	})

	It("WhereOr", func() {
		q := NewQuery(nil).Where("1 = 1").WhereOr("1 = 2")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT * WHERE (1 = 1) OR (1 = 2)`))
	})

	It("supports subqueries", func() {
		subq := NewQuery(nil, &SelectModel{}).Column("id").Where("name IS NOT NULL")
		q := NewQuery(nil).Where("id IN (?)", subq)

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT * WHERE (id IN (SELECT "id" FROM "select_models" AS "select_model" WHERE (name IS NOT NULL)))`))
	})

	It("supports locking", func() {
		q := NewQuery(nil).For("UPDATE SKIP LOCKED")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT * FOR UPDATE SKIP LOCKED`))
	})

	It("supports WhereGroup", func() {
		q := NewQuery(nil).Where("TRUE").WhereGroup(func(q *Query) (*Query, error) {
			q = q.Where("FALSE").WhereOr("TRUE")
			return q, nil
		})

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT * WHERE (TRUE) AND ((FALSE) OR (TRUE))`))
	})

	It("supports WhereOrGroup", func() {
		q := NewQuery(nil).Where("TRUE").WhereOrGroup(func(q *Query) (*Query, error) {
			q = q.Where("FALSE").Where("TRUE")
			return q, nil
		})

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT * WHERE (TRUE) OR ((FALSE) AND (TRUE))`))
	})

	It("supports empty WhereGroup", func() {
		q := NewQuery(nil).Where("TRUE").WhereGroup(func(q *Query) (*Query, error) {
			return q, nil
		})

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT * WHERE (TRUE)`))
	})

	It("expands ?TableAlias in Where with structs", func() {
		t := time.Date(2006, 2, 3, 10, 30, 35, 987654321, time.UTC)
		q := NewQuery(nil, &SelectModel{}).Column("id").Where("?TableAlias.name > ?", t)

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "id" FROM "select_models" AS "select_model" WHERE ("select_model".name > '2006-02-03 10:30:35.987654321+00:00:00')`))
	})

	It("supports DISTINCT", func() {
		q := NewQuery(nil, &SelectModel{}).Distinct()

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT DISTINCT "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model"`))
	})

	It("supports DISTINCT ON", func() {
		q := NewQuery(nil, &SelectModel{}).DistinctOn("expr(?)", "foo")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT DISTINCT ON (expr('foo')) "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model"`))
	})

	It("supports WhereIn", func() {
		q := NewQuery(nil).
			WhereIn("id IN (?)", []string{"foo", "bar"})

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT * WHERE (id IN ('foo','bar'))`))
	})

	It("supports Where & pg.In", func() {
		q := NewQuery(nil).
			Where("id IN (?)", types.In([]string{"foo", "bar"}))

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT * WHERE (id IN ('foo','bar'))`))
	})
})

var _ = Describe("Count", func() {
	It("removes LIMIT, OFFSET, and ORDER", func() {
		q := NewQuery(nil).Order("order").Limit(1).Offset(2)

		s := queryString(q.countSelectQuery("count(*)"))
		Expect(s).To(Equal(`SELECT count(*)`))
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

		s := queryString(q.countSelectQuery("count(*)"))
		Expect(s).To(Equal(`WITH "wrapper" AS (SELECT "col1", "col2" ORDER BY "order" LIMIT 1 OFFSET 2) SELECT count(*) FROM "wrapper"`))
	})

	It("includes has one joins", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).Relation("HasOne")

		s := queryString(q.countSelectQuery("count(*)"))
		Expect(s).To(Equal(`SELECT count(*) FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id"`))
	})

	It("uses CTE when query contains GROUP BY", func() {
		q := NewQuery(nil).Group("one")

		s := queryString(q.countSelectQuery("count(*)"))
		Expect(s).To(Equal(`WITH "_count_wrapper" AS (SELECT * GROUP BY "one") SELECT count(*) FROM "_count_wrapper"`))
	})

	It("uses CTE when column contains DISTINCT", func() {
		q := NewQuery(nil).ColumnExpr("DISTINCT group_id")

		s := queryString(q.countSelectQuery("count(*)"))
		Expect(s).To(Equal(`WITH "_count_wrapper" AS (SELECT DISTINCT group_id) SELECT count(*) FROM "_count_wrapper"`))
	})
})

var _ = Describe("With", func() {
	It("WrapWith wraps query in CTE", func() {
		q := NewQuery(nil, &SelectModel{}).
			Where("cond1").
			WrapWith("wrapper").
			Table("wrapper").
			Where("cond2")

		s := selectQueryString(q)
		Expect(s).To(Equal(`WITH "wrapper" AS (SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model" WHERE (cond1)) SELECT * FROM "wrapper" WHERE (cond2)`))
	})

	It("generates nested CTE", func() {
		q1 := NewQuery(nil).Table("q1")
		q2 := NewQuery(nil).With("q1", q1).Table("q2", "q1")
		q3 := NewQuery(nil).With("q2", q2).Table("q3", "q2")

		s := selectQueryString(q3)
		Expect(s).To(Equal(`WITH "q2" AS (WITH "q1" AS (SELECT * FROM "q1") SELECT * FROM "q2", "q1") SELECT * FROM "q3", "q2"`))
	})

	It("supports Join.JoinOn.JoinOnOr", func() {
		q := NewQuery(nil).Table("t1").
			Join("JOIN t2").JoinOn("t2.c1 = t1.c1").JoinOn("t2.c2 = t1.c1").
			Join("JOIN t3").JoinOn("t3.c1 = t3.c2").JoinOnOr("t3.c2 = t1.c2")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT * FROM "t1" JOIN t2 ON (t2.c1 = t1.c1) AND (t2.c2 = t1.c1) JOIN t3 ON (t3.c1 = t3.c2) OR (t3.c2 = t1.c2)`))
	})

	It("excludes a column", func() {
		q := NewQuery(nil, &SelectModel{}).
			ExcludeColumn("has_one_id")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "id", "name" FROM "select_models" AS "select_model"`))
	})

	It("supports WithDelete", func() {
		subq := NewQuery(nil, &SelectModel{}).
			Where("cond1")

		q := NewQuery(nil).
			WithDelete("wrapper", subq).
			Table("wrapper").
			Where("cond2")

		s := selectQueryString(q)
		Expect(s).To(Equal(`WITH "wrapper" AS (DELETE FROM "select_models" AS "select_model" WHERE (cond1)) SELECT * FROM "wrapper" WHERE (cond2)`))
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

			s := selectQueryString(q)
			Expect(s).To(Equal(`SELECT * ORDER BY ` + test.query))
		}
	})
})

type NonSoftDeleteModel struct {
	Id                int `pg:",pk"`
	Name              string
	SoftDeleteModelId int
	SoftDeleteModel   *SoftDeleteModel
}

type SoftDeleteModel struct {
	Id        int
	DeletedAt time.Time `pg:",soft_delete"`
}

type SoftDeleteParent struct {
	Id          uint64 `pg:"id,pk"`
	Name        string
	DateDeleted *time.Time `pg:",soft_delete"`

	Children *SoftDeleteChild
}

type SoftDeleteChild struct {
	Id                 uint64            `pg:"id,pk"`
	SoftDeleteParentId uint64            `pg:"soft_delete_parent_id,on_delete:CASCADE"`
	SoftDeleteParent   *SoftDeleteParent `pg:"-"`
	Name               string
	SubChildren        *SoftDeleteSubChild
}

type SoftDeleteSubChild struct {
	Id                uint64           `pg:"id,pk"`
	SoftDeleteChildId uint64           `pg:"soft_delete_child_id,on_delete:CASCADE"`
	SoftDeleteChild   *SoftDeleteChild `pg:"-"`
	Name              string
}

var _ = Describe("SoftDeleteModel", func() {
	It("filters out deleted rows by default", func() {
		q := NewQuery(nil, &SoftDeleteModel{})

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "soft_delete_model"."id", "soft_delete_model"."deleted_at" FROM "soft_delete_models" AS "soft_delete_model" WHERE "soft_delete_model"."deleted_at" IS NULL`))
	})

	It("supports Deleted", func() {
		q := NewQuery(nil, &SoftDeleteModel{}).Deleted()

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "soft_delete_model"."id", "soft_delete_model"."deleted_at" FROM "soft_delete_models" AS "soft_delete_model" WHERE "soft_delete_model"."deleted_at" IS NOT NULL`))
	})

	It("supports AllWithDeleted", func() {
		q := NewQuery(nil, &SoftDeleteModel{}).AllWithDeleted()

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "soft_delete_model"."id", "soft_delete_model"."deleted_at" FROM "soft_delete_models" AS "soft_delete_model"`))
	})

	It("will respect join SoftDelete", func() {
		q := NewQuery(nil, &SoftDeleteParent{}).Relation("Children").Relation("Children.SubChildren")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "soft_delete_parent"."id", "soft_delete_parent"."name", "soft_delete_parent"."date_deleted", "children"."id" AS "children__id", "children"."soft_delete_parent_id" AS "children__soft_delete_parent_id", "children"."name" AS "children__name", "children__sub_children"."id" AS "children__sub_children__id", "children__sub_children"."soft_delete_child_id" AS "children__sub_children__soft_delete_child_id", "children__sub_children"."name" AS "children__sub_children__name" FROM "soft_delete_parents" AS "soft_delete_parent" LEFT JOIN "soft_delete_children" AS "children" ON "children"."soft_delete_parent_id" = "soft_delete_parent"."id" LEFT JOIN "soft_delete_sub_children" AS "children__sub_children" ON "children__sub_children"."soft_delete_child_id" = "children"."id" WHERE "soft_delete_parent"."date_deleted" IS NULL`))
	})

	It("will join a non-SoftDelete with a SoftDelete", func() {
		q := NewQuery(nil, &NonSoftDeleteModel{}).Relation("SoftDeleteModel")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "non_soft_delete_model"."id", "non_soft_delete_model"."name", "non_soft_delete_model"."soft_delete_model_id", "soft_delete_model"."id" AS "soft_delete_model__id", "soft_delete_model"."deleted_at" AS "soft_delete_model__deleted_at" FROM "non_soft_delete_models" AS "non_soft_delete_model" LEFT JOIN "soft_delete_models" AS "soft_delete_model" ON ("soft_delete_model"."id" = "non_soft_delete_model"."soft_delete_model_id") AND "soft_delete_model"."deleted_at" IS NULL`))
	})
})

var _ = Describe("union", func() {
	It("simple", func() {
		q1 := NewQuery(nil).ColumnExpr("1").OrderExpr("1 ASC")
		q2 := NewQuery(nil).ColumnExpr("2").OrderExpr("1 ASC")

		s := selectQueryString(q1.Union(q2))
		Expect(s).To(Equal(`(SELECT 1 ORDER BY 1 ASC) UNION (SELECT 2 ORDER BY 1 ASC)`))
	})

	It("manual", func() {
		q1 := NewQuery(nil).ColumnExpr("1").OrderExpr("1 ASC")
		q2 := NewQuery(nil).ColumnExpr("2").OrderExpr("1 ASC")
		q := NewQuery(nil).
			ColumnExpr("(?) UNION ALL (?)", q1, q2).
			OrderExpr("1 DESC")

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT (SELECT 1 ORDER BY 1 ASC) UNION ALL (SELECT 2 ORDER BY 1 ASC) ORDER BY 1 DESC`))
	})
})

func selectQueryString(q *Query) string {
	sel := NewSelectQuery(q)
	s := queryString(sel)
	return s
}

func queryString(f QueryAppender) string {
	fmter := NewFormatter().WithModel(f)
	b, err := f.AppendQuery(fmter, nil)
	Expect(err).NotTo(HaveOccurred())
	return string(b)
}
