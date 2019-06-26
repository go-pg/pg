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
		q := NewQuery(nil, &UpdateTest{}).WherePK()

		s := updateQueryString(q)
		Expect(s).To(Equal(`UPDATE "update_tests" AS "update_test" SET "value" = NULL WHERE "update_test"."id" = NULL`))
	})

	It("supports Value", func() {
		model := &UpdateTest{
			Id:    1,
			Value: "hello",
		}
		q := NewQuery(nil, model).
			Value("value", "upper(?)", model.Value).
			WherePK()

		s := updateQueryString(q)
		Expect(s).To(Equal(`UPDATE "update_tests" AS "update_test" SET "value" = upper('hello') WHERE "update_test"."id" = 1`))
	})

	It("supports Value 2", func() {
		model := &UpdateTest{
			Id:    1,
			Value: "hello",
		}
		q := NewQuery(nil, model).Value("value", "upper(?value)").WherePK()

		s := updateQueryString(q)
		Expect(s).To(Equal(`UPDATE "update_tests" AS "update_test" SET "value" = upper('hello') WHERE "update_test"."id" = 1`))
	})

	It("omits zero", func() {
		q := NewQuery(nil, &UpdateTest{}).WherePK()

		s := queryString(&updateQuery{q: q, omitZero: true})
		Expect(s).To(Equal(`UPDATE "update_tests" AS "update_test" SET  WHERE "update_test"."id" = NULL`))
	})

	It("bulk updates", func() {
		q := NewQuery(nil, &UpdateTest{
			Id:    1,
			Value: "hello",
		}, &UpdateTest{
			Id: 2,
		})

		s := updateQueryString(q)
		Expect(s).To(Equal(`UPDATE "update_tests" AS "update_test" SET "value" = _data."value" FROM (VALUES (1, 'hello'::mytype), (2, NULL::mytype)) AS _data("id", "value") WHERE "update_test"."id" = _data."id"`))
	})

	It("bulk updates overriding column value", func() {
		slice := []*UpdateTest{{
			Id:    1,
			Value: "hello",
		}, {
			Id: 2,
		}}
		q := NewQuery(nil, &slice).Value("id", "123")

		s := updateQueryString(q)
		Expect(s).To(Equal(`UPDATE "update_tests" AS "update_test" SET "value" = _data."value" FROM (VALUES (123, 'hello'::mytype), (123, NULL::mytype)) AS _data("id", "value") WHERE "update_test"."id" = _data."id"`))
	})

	It("returns an error for empty bulk update", func() {
		slice := make([]UpdateTest, 0)
		q := NewQuery(nil, &slice)

		_, err := newUpdateQuery(q, false).AppendQuery(defaultFmter, nil)
		Expect(err).To(MatchError("pg: can't bulk-update empty slice []orm.UpdateTest"))
	})

	It("supports WITH", func() {
		q := NewQuery(nil, &UpdateTest{}).
			WrapWith("wrapper").
			Model(&UpdateTest{}).
			Table("wrapper").
			Where("update_test.id = wrapper.id")

		s := updateQueryString(q)
		Expect(s).To(Equal(`WITH "wrapper" AS (SELECT "update_test"."id", "update_test"."value" FROM "update_tests" AS "update_test") UPDATE "update_tests" AS "update_test" SET "value" = NULL FROM "wrapper" WHERE (update_test.id = wrapper.id)`))
	})

	It("supports notnull and default", func() {
		type Model struct {
			Id   int
			Bool bool `sql:",notnull,default:_"`
		}

		q := NewQuery(nil, &Model{}).WherePK()

		s := updateQueryString(q)
		Expect(s).To(Equal(`UPDATE "models" AS "model" SET "bool" = FALSE WHERE "model"."id" = NULL`))
	})

	It("allows disabling an alias", func() {
		type Model struct {
			tableName struct{} `sql:"alias:models"`

			Id int
		}

		q := NewQuery(nil, &Model{}).WherePK()

		s := updateQueryString(q)
		Expect(s).To(Equal(`UPDATE "models" SET  WHERE "models"."id" = NULL`))
	})
})

func updateQueryString(q *Query) string {
	upd := newUpdateQuery(q, false)
	s := queryString(upd)
	return s
}
