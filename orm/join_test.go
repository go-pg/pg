package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type JoinTest struct {
	tableName struct{} `sql:"JoinTest,alias:JoinTest"`

	Id int

	HasOne   *HasOne
	HasOneId int

	BelongsTo *BelongsTo
}

type HasOne struct {
	tableName struct{} `sql:"HasOne,alias:HasOne"`

	Id int

	HasOne   *HasOne
	HasOneId int
}

type BelongsTo struct {
	tableName struct{} `sql:"BelongsTo,alias:BelongsTo"`

	Id         int
	JoinTestId int
}

var _ = Describe("Select", func() {
	It("supports has one", func() {
		q := NewQuery(nil, &JoinTest{}).Relation("HasOne.HasOne", nil)

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT JoinTest."id", JoinTest."has_one_id", "has_one"."id" AS "has_one__id", "has_one"."has_one_id" AS "has_one__has_one_id", "has_one__has_one"."id" AS "has_one__has_one__id", "has_one__has_one"."has_one_id" AS "has_one__has_one__has_one_id" FROM JoinTest AS JoinTest LEFT JOIN HasOne AS "has_one" ON "has_one"."id" = JoinTest."has_one_id" LEFT JOIN HasOne AS "has_one__has_one" ON "has_one__has_one"."id" = "has_one"."has_one_id"`))
	})

	It("supports belongs to", func() {
		q := NewQuery(nil, &JoinTest{}).Relation("BelongsTo", nil)

		b, err := (&selectQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT JoinTest."id", JoinTest."has_one_id", "belongs_to"."id" AS "belongs_to__id", "belongs_to"."join_test_id" AS "belongs_to__join_test_id" FROM JoinTest AS JoinTest LEFT JOIN BelongsTo AS "belongs_to" ON "belongs_to"."join_test_id" = JoinTest."id"`))
	})
})
