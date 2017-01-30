package orm

import (
	"database/sql"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type CreateTableModel struct {
	Id          int
	Int8        int8
	Uint8       uint8
	Int16       int16
	Uint16      uint16
	Int32       int32
	Uint32      uint32
	Int64       int64
	Uint64      uint64
	Float32     float32
	Float64     float64
	String      string
	Varchar     string `sql:",type:varchar(500)"`
	Time        time.Time
	NotNull     int `sql:",notnull"`
	Unique      int `sql:",unique"`
	NullBool    sql.NullBool
	NullFloat64 sql.NullFloat64
	NullInt64   sql.NullInt64
	NullString  sql.NullString
	Slice       []int
	Map         map[int]int
	Struct      struct{}
}

type CreateTableWithoutPKModel struct {
	String string
}

var _ = Describe("CreateTable", func() {
	It("creates new table", func() {
		b, err := createTableQuery{model: CreateTableModel{}}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`CREATE TABLE "create_table_models" (id bigserial, int8 smallint, uint8 smallint, int16 smallint, uint16 integer, int32 integer, uint32 bigint, int64 bigint, uint64 decimal, float32 real, float64 double precision, string text, varchar varchar(500), time timestamptz, not_null bigint NOT NULL, unique bigint UNIQUE, null_bool boolean, null_float64 double precision, null_int64 bigint, null_string text, slice jsonb, map jsonb, struct jsonb, PRIMARY KEY (id))`))
	})

	It("creates new table without primary key", func() {
		b, err := createTableQuery{model: CreateTableWithoutPKModel{}}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`CREATE TABLE "create_table_without_pk_models" (string text)`))
	})
})
