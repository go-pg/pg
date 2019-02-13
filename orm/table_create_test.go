package orm

import (
	"database/sql"
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type CreateTableModel struct {
	Id             int
	Int8           int8
	Uint8          uint8
	Int16          int16
	Uint16         uint16
	Int32          int32
	Uint32         uint32
	Int64          int64
	Uint64         uint64
	Float32        float32
	Float64        float64
	Decimal        float64 `sql:"type:'decimal(10,10)'"`
	ByteSlice      []byte
	ByteArray      [32]byte
	String         string    `sql:"default:'D\\'Angelo'"`
	Varchar        string    `sql:",type:varchar(500)"`
	Time           time.Time `sql:"default:now()"`
	NotNull        int       `sql:",notnull"`
	NullBool       sql.NullBool
	NullFloat64    sql.NullFloat64
	NullInt64      sql.NullInt64
	NullString     sql.NullString
	Slice          []int
	SliceArray     []int `sql:",array"`
	Map            map[int]int
	MapHstore      map[int]int `sql:",hstore"`
	Struct         struct{}
	StructPtr      *struct{}
	Unique         int `sql:",unique"`
	UniqueField1   int `sql:"unique:field1_field2"`
	UniqueField2   int `sql:"unique:field1_field2"`
	JSONRawMessage json.RawMessage
}

type CreateTableWithoutPKModel struct {
	String string
}

type CreateTableOnDeleteOnUpdateModel struct {
	Id                 int
	CreateTableModelId int `sql:"on_delete:RESTRICT, on_update:CASCADE"`
	CreateTableModel   *CreateTableModel
}

type CreateTableWithTablespace struct {
	tableName string `sql:"tablespace:ssd"`

	String string
}

var _ = Describe("CreateTable", func() {
	It("creates new table", func() {
		q := NewQuery(nil, &CreateTableModel{})

		b, err := (&createTableQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`CREATE TABLE "create_table_models" ("id" bigserial, "int8" smallint, "uint8" smallint, "int16" smallint, "uint16" integer, "int32" integer, "uint32" bigint, "int64" bigint, "uint64" bigint, "float32" real, "float64" double precision, "decimal" decimal(10,10), "byte_slice" bytea, "byte_array" bytea, "string" text DEFAULT 'D''Angelo', "varchar" varchar(500), "time" timestamptz DEFAULT now(), "not_null" bigint NOT NULL, "null_bool" boolean, "null_float64" double precision, "null_int64" bigint, "null_string" text, "slice" jsonb, "slice_array" bigint[], "map" jsonb, "map_hstore" hstore, "struct" jsonb, "struct_ptr" jsonb, "unique" bigint UNIQUE, "unique_field1" bigint, "unique_field2" bigint, "json_raw_message" jsonb, PRIMARY KEY ("id"), UNIQUE ("unique_field1", "unique_field2"))`))
	})

	It("creates new table without primary key", func() {
		q := NewQuery(nil, &CreateTableWithoutPKModel{})

		b, err := (&createTableQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`CREATE TABLE "create_table_without_pk_models" ("string" text)`))
	})

	It("creates new table with Varchar=255", func() {
		q := NewQuery(nil, &CreateTableWithoutPKModel{})

		opt := &CreateTableOptions{Varchar: 255}
		b, err := (&createTableQuery{q: q, opt: opt}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`CREATE TABLE "create_table_without_pk_models" ("string" varchar(255))`))
	})

	It("creates new table with on_delete and on_update options", func() {
		q := NewQuery(nil, &CreateTableOnDeleteOnUpdateModel{})

		opt := &CreateTableOptions{FKConstraints: true}
		b, err := (&createTableQuery{q: q, opt: opt}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`CREATE TABLE "create_table_on_delete_on_update_models" ("id" bigserial, "create_table_model_id" bigint, PRIMARY KEY ("id"), FOREIGN KEY ("create_table_model_id") REFERENCES "create_table_models" ("id") ON DELETE RESTRICT ON UPDATE CASCADE)`))
	})

	It("creates new table with tablespace options", func() {
		q := NewQuery(nil, &CreateTableWithTablespace{})

		opt := &CreateTableOptions{}
		b, err := (&createTableQuery{q: q, opt: opt}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`CREATE TABLE "create_table_with_tablespaces" ("string" text) TABLESPACE ssd`))
	})
})
