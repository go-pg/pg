package orm

import (
	"database/sql"
	"time"

	"github.com/go-pg/pg/v10/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type UpdateTest struct {
	Id    int
	Value string `pg:"type:mytype"`
}

type SerialUpdateTest struct {
	Id    uint64 `pg:"type:bigint,pk"`
	Value string
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

		s := queryString(&UpdateQuery{q: q, omitZero: true})
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
		Expect(s).To(Equal(`UPDATE "update_tests" AS "update_test" SET "value" = _data."value" FROM (VALUES (1::bigint, 'hello'::mytype), (2::bigint, NULL::mytype)) AS _data("id", "value") WHERE "update_test"."id" = _data."id"`))
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

	It("bulk updates with zero values", func() {
		slice := []*UpdateTest{{
			Id:    1,
			Value: "hello",
		}, {
			Id: 2,
		}}
		q := NewQuery(nil, &slice)

		s := updateQueryStringOmitZero(q)
		Expect(s).To(Equal(`UPDATE "update_tests" AS "update_test" SET "value" = COALESCE(_data."value", "update_test"."value") FROM (VALUES (1::bigint, 'hello'::mytype), (2::bigint, NULL::mytype)) AS _data("id", "value") WHERE "update_test"."id" = _data."id"`))
	})

	It("bulk updates with serial id", func() {
		slice := []*SerialUpdateTest{{
			Id:    1,
			Value: "hello",
		}}
		q := NewQuery(nil, &slice)

		s := updateQueryString(q)
		Expect(s).To(Equal(`UPDATE "serial_update_tests" AS "serial_update_test" SET "value" = _data."value" FROM (VALUES (1::bigint, 'hello'::text)) AS _data("id", "value") WHERE "serial_update_test"."id" = _data."id"`))
	})

	It("returns an error for empty bulk update", func() {
		slice := make([]UpdateTest, 0)
		q := NewQuery(nil, &slice)

		_, err := NewUpdateQuery(q, false).AppendQuery(defaultFmter, nil)
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

	It("supports use_zero and default tags", func() {
		type Model struct {
			Id   int
			Bool bool `pg:",default:_,use_zero"`
		}

		q := NewQuery(nil, &Model{}).WherePK()

		s := updateQueryString(q)
		Expect(s).To(Equal(`UPDATE "models" AS "model" SET "bool" = FALSE WHERE "model"."id" = NULL`))
	})

	It("allows disabling an alias", func() {
		type Model struct {
			tableName struct{} `pg:"alias:models"`

			Id int
		}

		q := NewQuery(nil, &Model{}).WherePK()

		s := updateQueryString(q)
		Expect(s).To(Equal(`UPDATE "models" SET  WHERE "models"."id" = NULL`))
	})

	It("omits zero values", func() {
		type Model struct {
			ID   int
			Str  string
			Bool *bool
		}

		{
			q := NewQuery(nil, &Model{ID: 1, Str: "hello"}).WherePK()

			s := updateQueryStringOmitZero(q)
			Expect(s).To(Equal(`UPDATE "models" AS "model" SET "str" = 'hello' WHERE "model"."id" = 1`))
		}

		{
			q := NewQuery(nil, &Model{ID: 1, Bool: new(bool)}).WherePK()

			s := updateQueryStringOmitZero(q)
			Expect(s).To(Equal(`UPDATE "models" AS "model" SET "bool" = FALSE WHERE "model"."id" = 1`))
		}
	})

	It("bulk updates pg.NullTime", func() {
		type Model struct {
			ID        int64          `pg:"id,pk"`
			CreatedAt types.NullTime `pg:"created_at"`
			DeletedAt sql.NullTime   `pg:"deleted_at"`
		}

		q := NewQuery(nil, &[]Model{{
			ID:        1,
			CreatedAt: types.NullTime{time.Unix(0, 0)},
			DeletedAt: sql.NullTime{Time: time.Unix(0, 0), Valid: true},
		}})

		s := updateQueryString(q)
		Expect(s).To(Equal(`UPDATE "models" AS "model" SET "created_at" = _data."created_at", "deleted_at" = _data."deleted_at" FROM (VALUES (1::bigint, '1970-01-01 00:00:00+00:00:00'::timestamptz, '1970-01-01 00:00:00+00:00:00'::timestamptz)) AS _data("id", "created_at", "deleted_at") WHERE "model"."id" = _data."id"`))
	})

	It("updates map[string]interface{}", func() {
		q := NewQuery(nil, &map[string]interface{}{
			"hello": "world",
			"foo":   123,
			"bar":   time.Unix(0, 0),
			"nil":   nil,
		}).
			TableExpr("my_table").
			Where("id = 1")

		s := updateQueryString(q)
		Expect(s).To(Equal(`UPDATE my_table SET "bar" = '1970-01-01 00:00:00+00:00:00', "foo" = 123, "hello" = 'world', "nil" = NULL WHERE (id = 1)`))
	})
})

func updateQueryString(q *Query) string {
	upd := NewUpdateQuery(q, false)
	s := queryString(upd)
	return s
}

func updateQueryStringOmitZero(q *Query) string {
	upd := NewUpdateQuery(q, true)
	s := queryString(upd)
	return s
}
