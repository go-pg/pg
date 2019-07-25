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
	Duration       time.Duration
	Nullable       int `sql:",nullable"`
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

type CreateTableWithRangePartition struct {
	tableName string `sql:"partitionBy:RANGE (time)"`

	Time   time.Time
	String string
}

type CreateTableWithListPartition struct {
	tableName string `sql:"partitionBy:LIST (country)"`

	Country string
	String  string
}

type CreateTableWithHashPartition struct {
	tableName string `sql:"partitionBy:HASH (account_id)"`

	ID        int
	AccountID int
	String    string
}

var _ = Describe("CreateTable", func() {
	It("creates new table", func() {
		q := NewQuery(nil, &CreateTableModel{})

		s := createTableQueryString(q, nil)
		Expect(s).To(Equal(`CREATE TABLE "create_table_models" ("id" bigserial, "int8" smallint NOT NULL, "uint8" smallint NOT NULL, "int16" smallint NOT NULL, "uint16" integer NOT NULL, "int32" integer NOT NULL, "uint32" bigint NOT NULL, "int64" bigint NOT NULL, "uint64" bigint NOT NULL, "float32" real NOT NULL, "float64" double precision NOT NULL, "decimal" decimal(10,10) NOT NULL, "byte_slice" bytea NOT NULL, "byte_array" bytea NOT NULL, "string" text NOT NULL DEFAULT 'D''Angelo', "varchar" varchar(500) NOT NULL, "time" timestamptz NOT NULL DEFAULT now(), "duration" bigint NOT NULL, "nullable" bigint, "null_bool" boolean NOT NULL, "null_float64" double precision NOT NULL, "null_int64" bigint NOT NULL, "null_string" text NOT NULL, "slice" jsonb NOT NULL, "slice_array" bigint[] NOT NULL, "map" jsonb NOT NULL, "map_hstore" hstore NOT NULL, "struct" jsonb NOT NULL, "struct_ptr" jsonb NOT NULL, "unique" bigint NOT NULL UNIQUE, "unique_field1" bigint NOT NULL, "unique_field2" bigint NOT NULL, "json_raw_message" jsonb NOT NULL, PRIMARY KEY ("id"), UNIQUE ("unique_field1", "unique_field2"))`))
	})

	It("creates new table without primary key", func() {
		q := NewQuery(nil, &CreateTableWithoutPKModel{})

		s := createTableQueryString(q, nil)
		Expect(s).To(Equal(`CREATE TABLE "create_table_without_pk_models" ("string" text NOT NULL)`))
	})

	It("creates new table with Varchar=255", func() {
		q := NewQuery(nil, &CreateTableWithoutPKModel{})

		s := createTableQueryString(q, &CreateTableOptions{Varchar: 255})
		Expect(s).To(Equal(`CREATE TABLE "create_table_without_pk_models" ("string" varchar(255) NOT NULL)`))
	})

	It("creates new table with on_delete and on_update options", func() {
		q := NewQuery(nil, &CreateTableOnDeleteOnUpdateModel{})

		s := createTableQueryString(q, &CreateTableOptions{FKConstraints: true})
		Expect(s).To(Equal(`CREATE TABLE "create_table_on_delete_on_update_models" ("id" bigserial, "create_table_model_id" bigint NOT NULL, PRIMARY KEY ("id"), FOREIGN KEY ("create_table_model_id") REFERENCES "create_table_models" ("id") ON DELETE RESTRICT ON UPDATE CASCADE)`))
	})

	It("creates new table with tablespace options", func() {
		q := NewQuery(nil, &CreateTableWithTablespace{})

		s := createTableQueryString(q, &CreateTableOptions{})
		Expect(s).To(Equal(`CREATE TABLE "create_table_with_tablespaces" ("string" text NOT NULL) TABLESPACE "ssd"`))
	})

	It("creates new table with range partition", func() {
		q := NewQuery(nil, &CreateTableWithRangePartition{})

		s := createTableQueryString(q, &CreateTableOptions{})
		Expect(s).To(Equal(`CREATE TABLE "create_table_with_range_partitions" ("time" timestamptz NOT NULL, "string" text NOT NULL) PARTITION BY RANGE (time)`))
	})

	It("creates new table with list partition", func() {
		q := NewQuery(nil, &CreateTableWithListPartition{})

		s := createTableQueryString(q, &CreateTableOptions{})
		Expect(s).To(Equal(`CREATE TABLE "create_table_with_list_partitions" ("country" text NOT NULL, "string" text NOT NULL) PARTITION BY LIST (country)`))
	})

	It("creates new table with hash partition", func() {
		q := NewQuery(nil, &CreateTableWithHashPartition{})

		s := createTableQueryString(q, &CreateTableOptions{})
		Expect(s).To(Equal(`CREATE TABLE "create_table_with_hash_partitions" ("id" bigserial, "account_id" bigint NOT NULL, "string" text NOT NULL, PRIMARY KEY ("id")) PARTITION BY HASH (account_id)`))
	})
})

func createTableQueryString(q *Query, opt *CreateTableOptions) string {
	qq := newCreateTableQuery(q, opt)
	return queryString(qq)
}
