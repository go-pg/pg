package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type UpdateTest struct {
	Id    int
	Value string
}

var _ = Describe("Update", func() {
	It("multi updates", func() {
		q := NewQuery(nil, &UpdateTest{}).
			Model(&UpdateTest{
				Id:    1,
				Value: "hello",
			}, &UpdateTest{
				Id: 2,
			})

		b, err := updateQuery{q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`UPDATE "update_tests" AS "update_test" SET "value" = _data."value" FROM (VALUES (1, 'hello'), (2, NULL)) AS _data("id", "value") WHERE "update_test"."id" = _data."id"`))
	})

	It("supports WITH", func() {
		q := NewQuery(nil, &UpdateTest{}).
			WrapWith("wrapper").
			Model(&UpdateTest{}).
			Table("wrapper").
			Where("update_test.id = wrapper.id")

		b, err := updateQuery{q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`WITH "wrapper" AS (SELECT "update_test"."id", "update_test"."value" FROM "update_tests" AS "update_test") UPDATE "update_tests" AS "update_test" SET "value" = NULL FROM "wrapper" WHERE (update_test.id = wrapper.id)`))
	})
})
