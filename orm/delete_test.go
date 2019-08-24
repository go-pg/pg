package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type DeleteTest struct{}

var _ = Describe("Delete", func() {
	It("supports WITH", func() {
		q := NewQuery(nil, &DeleteTest{}).
			WrapWith("wrapper").
			Model(&DeleteTest{}).
			Table("wrapper").
			Where("delete_test.id = wrapper.id")

		s := deleteQueryString(q)
		Expect(s).To(Equal(`WITH "wrapper" AS (SELECT  FROM "delete_tests" AS "delete_test") DELETE FROM "delete_tests" AS "delete_test" USING "wrapper" WHERE (delete_test.id = wrapper.id)`))
	})

	It("supports array with where", func() {
		type DeleteWithPk struct {
			Id        uint64 `sql:",pk"`
			AccountId uint64
			Name      string
		}

		q := NewQuery(nil, &[]DeleteWithPk{
			{
				Id: 1,
			},
			{
				Id: 2,
			},
		}).Where("account_id = ?", 1)

		s := deleteQueryString(q)
		Expect(s).To(Equal(`DELETE FROM "delete_with_pks" AS "delete_with_pk" WHERE "delete_with_pk"."id" IN (1, 2) AND (account_id = 1)`))
	})
})

func deleteQueryString(q *Query) string {
	del := newDeleteQuery(q)
	return queryString(del)
}
