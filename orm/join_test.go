package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type JoinTest struct {
	tableName struct{} `pg:"JoinTest,alias:JoinTest"`

	Id int

	HasOne   *HasOne
	HasOneId int

	BelongsTo *BelongsTo
}

type HasOne struct {
	tableName struct{} `pg:"HasOne,alias:HasOne"`

	Id int

	HasOne   *HasOne
	HasOneId int
}

type BelongsTo struct {
	tableName struct{} `pg:"BelongsTo,alias:BelongsTo"`

	Id         int
	JoinTestId int
}

type HasOneNonPK struct {
	tableName struct{} `pg:"HasOneNonPK,alias:HasOneNonPK"`

	Id int

	CustomKey string `pg:"custom_key"`
}

type NonPKJoinTest struct {
	tableName struct{} `pg:"NonPKJoinTest,alias:NonPKJoinTest"`

	Id int

	CustomHasOneKey string  `pg:"custom_has_one_key"`
	CustomHasOne    *HasOneNonPK `pg:"rel:has-one,fk:custom_has_one_key,join_fk:custom_key"`
}

var _ = Describe("Join", func() {
	It("supports has one", func() {
		q := NewQuery(nil, &JoinTest{}).Relation("HasOne.HasOne", nil)

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "JoinTest"."id", "JoinTest"."has_one_id", "has_one"."id" AS "has_one__id", "has_one"."has_one_id" AS "has_one__has_one_id", "has_one__has_one"."id" AS "has_one__has_one__id", "has_one__has_one"."has_one_id" AS "has_one__has_one__has_one_id" FROM "JoinTest" AS "JoinTest" LEFT JOIN "HasOne" AS "has_one" ON "has_one"."id" = "JoinTest"."has_one_id" LEFT JOIN "HasOne" AS "has_one__has_one" ON "has_one__has_one"."id" = "has_one"."has_one_id"`))
	})

	It("supports join_fk for has one", func() {
		q := NewQuery(nil, &NonPKJoinTest{}).Relation("CustomHasOne", nil)

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "NonPKJoinTest"."id", "NonPKJoinTest"."custom_has_one_key", "custom_has_one"."id" AS "custom_has_one__id", "custom_has_one"."custom_key" AS "custom_has_one__custom_key" FROM "NonPKJoinTest" AS "NonPKJoinTest" LEFT JOIN "HasOneNonPK" AS "custom_has_one" ON "custom_has_one"."custom_key" = "NonPKJoinTest"."custom_has_one_key"`))
	})

	It("supports belongs to", func() {
		q := NewQuery(nil, &JoinTest{}).Relation("BelongsTo", nil)

		s := selectQueryString(q)
		Expect(s).To(Equal(`SELECT "JoinTest"."id", "JoinTest"."has_one_id", "belongs_to"."id" AS "belongs_to__id", "belongs_to"."join_test_id" AS "belongs_to__join_test_id" FROM "JoinTest" AS "JoinTest" LEFT JOIN "BelongsTo" AS "belongs_to" ON "belongs_to"."join_test_id" = "JoinTest"."id"`))
	})
})
