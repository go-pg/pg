package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type UpdateTest struct {
	Id    int
	Value string `sql:"type:mytype"`
}

var _ = Describe("Update", func() {
	It("updates model", func() {
		q := NewQuery(nil, &UpdateTest{})

		b, err := updateQuery{q: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`UPDATE "update_tests" AS "update_test" SET "value" = NULL WHERE "update_test"."id" = NULL`))
	})

	It("omits zero", func() {
		q := NewQuery(nil, &UpdateTest{})

		b, err := updateQuery{q: q, omitZero: true}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`UPDATE "update_tests" AS "update_test" SET  WHERE "update_test"."id" = NULL`))
	})

	It("bulk updates", func() {
		q := NewQuery(nil, &UpdateTest{
			Id:    1,
			Value: "hello",
		}, &UpdateTest{
			Id: 2,
		})

		b, err := updateQuery{q: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`UPDATE "update_tests" AS "update_test" SET "value" = _data."value" FROM (VALUES (1, 'hello'::mytype), (2, NULL::mytype)) AS _data("id", "value") WHERE "update_test"."id" = _data."id"`))
	})

	It("bulk updates with empty slice", func() {
		slice := make([]UpdateTest, 0)
		q := NewQuery(nil, &slice)

		_, err := updateQuery{q: q}.AppendQuery(nil)
		Expect(err).To(MatchError("pg: slice []orm.UpdateTest is empty"))
	})

	It("supports WITH", func() {
		q := NewQuery(nil, &UpdateTest{}).
			WrapWith("wrapper").
			Model(&UpdateTest{}).
			Table("wrapper").
			Where("update_test.id = wrapper.id")

		b, err := updateQuery{q: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`WITH "wrapper" AS (SELECT "update_test"."id", "update_test"."value" FROM "update_tests" AS "update_test") UPDATE "update_tests" AS "update_test" SET "value" = NULL FROM "wrapper" WHERE (update_test.id = wrapper.id)`))
	})
})
