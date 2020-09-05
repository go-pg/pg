package orm

import (
	"database/sql"
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type CreateTableModel struct {
	ID             int
	Serial         int
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
	Decimal        float64 `pg:"type:'decimal(10,10)'"`
	ByteSlice      []byte
	ByteArray      [32]byte
	String         string    `pg:"default:'D\\'Angelo'"`
	Varchar        string    `pg:",type:varchar(500)"`
	Time           time.Time `pg:"default:now()"`
	Duration       time.Duration
	NotNull        int `pg:",notnull"`
	NullBool       sql.NullBool
	NullFloat64    sql.NullFloat64
	NullInt64      sql.NullInt64
	NullString     sql.NullString
	Slice          []int
	SliceArray     []int `pg:",array"`
	Map            map[int]int
	MapHstore      map[int]int `pg:",hstore"`
	Struct         struct{}
	StructPtr      *struct{}
	Unique         int `pg:",unique"`
	UniqueField1   int `pg:"unique:field1_field2"`
	UniqueField2   int `pg:"unique:field1_field2"`
	JSONRawMessage json.RawMessage
}

type CreateTableWithoutPKModel struct {
	String string
}

type CreateTableOnDeleteOnUpdateModel struct {
	ID                 int
	CreateTableModelID int `pg:"on_delete:RESTRICT, on_update:CASCADE"`
	CreateTableModel   *CreateTableModel
}

type CreateTableWithTablespace struct {
	tableName string `pg:"tablespace:ssd"`

	String string
}

type CreateTableWithRangePartition struct {
	tableName string `pg:"partition_by:RANGE (time)"`

	Time   time.Time
	String string
}

type CreateTableWithListPartition struct {
	tableName string `pg:"partition_by:LIST (country)"`

	Country string
	String  string
}

type CreateTableWithHashPartition struct {
	tableName string `pg:"partition_by:HASH (account_id)"`

	ID        int `pg:",pk,type:int,default:0"`
	AccountID int
	String    string
}

type CreateTableWithMultipleNamedUniques struct {
	ID               int    `pg:",pk"`
	AccountID        int    `pg:",unique:'per_account,per_store'"`
	OrderNumber      string `pg:",unique:per_account"`
	StoreOrderNumber string `pg:",unique:per_store"`
}

var _ = Describe("CreateTable", func() {
	It("creates new table", func() {
		q := NewQuery(nil, &CreateTableModel{})

		s := createTableQueryString(q, nil)
		Expect(s).To(Equal(`CREATE TABLE "create_table_models" ("id" bigserial, "serial" bigint, "int8" smallint, "uint8" smallint, "int16" smallint, "uint16" integer, "int32" integer, "uint32" bigint, "int64" bigint, "uint64" bigint, "float32" real, "float64" double precision, "decimal" decimal(10,10), "byte_slice" bytea, "byte_array" bytea, "string" text DEFAULT 'D''Angelo', "varchar" varchar(500), "time" timestamptz DEFAULT now(), "duration" bigint, "not_null" bigint NOT NULL, "null_bool" boolean, "null_float64" double precision, "null_int64" bigint, "null_string" text, "slice" jsonb, "slice_array" bigint[], "map" jsonb, "map_hstore" hstore, "struct" jsonb, "struct_ptr" jsonb, "unique" bigint UNIQUE, "unique_field1" bigint, "unique_field2" bigint, "json_raw_message" jsonb, PRIMARY KEY ("id"), UNIQUE ("unique"), UNIQUE ("unique_field1", "unique_field2"))`))
	})

	It("creates new table without primary key", func() {
		q := NewQuery(nil, &CreateTableWithoutPKModel{})

		s := createTableQueryString(q, nil)
		Expect(s).To(Equal(`CREATE TABLE "create_table_without_pk_models" ("string" text)`))
	})

	It("creates new table with Varchar=255", func() {
		q := NewQuery(nil, &CreateTableWithoutPKModel{})

		s := createTableQueryString(q, &CreateTableOptions{Varchar: 255})
		Expect(s).To(Equal(`CREATE TABLE "create_table_without_pk_models" ("string" varchar(255))`))
	})

	It("creates new table with on_delete and on_update options", func() {
		q := NewQuery(nil, &CreateTableOnDeleteOnUpdateModel{})

		s := createTableQueryString(q, &CreateTableOptions{FKConstraints: true})
		Expect(s).To(Equal(`CREATE TABLE "create_table_on_delete_on_update_models" ("id" bigserial, "create_table_model_id" bigint, PRIMARY KEY ("id"), FOREIGN KEY ("create_table_model_id") REFERENCES "create_table_models" ("id") ON DELETE RESTRICT ON UPDATE CASCADE)`))
	})

	It("creates new table with tablespace options", func() {
		q := NewQuery(nil, &CreateTableWithTablespace{})

		s := createTableQueryString(q, &CreateTableOptions{})
		Expect(s).To(Equal(`CREATE TABLE "create_table_with_tablespaces" ("string" text) TABLESPACE "ssd"`))
	})

	It("creates new table with range partition", func() {
		q := NewQuery(nil, &CreateTableWithRangePartition{})

		s := createTableQueryString(q, &CreateTableOptions{})
		Expect(s).To(Equal(`CREATE TABLE "create_table_with_range_partitions" ("time" timestamptz, "string" text) PARTITION BY RANGE (time)`))
	})

	It("creates new table with list partition", func() {
		q := NewQuery(nil, &CreateTableWithListPartition{})

		s := createTableQueryString(q, &CreateTableOptions{})
		Expect(s).To(Equal(`CREATE TABLE "create_table_with_list_partitions" ("country" text, "string" text) PARTITION BY LIST (country)`))
	})

	It("creates new table with hash partition", func() {
		q := NewQuery(nil, &CreateTableWithHashPartition{})

		s := createTableQueryString(q, &CreateTableOptions{})
		Expect(s).To(Equal(`CREATE TABLE "create_table_with_hash_partitions" ("id" int DEFAULT 0, "account_id" bigint, "string" text, PRIMARY KEY ("id")) PARTITION BY HASH (account_id)`))
	})

	It("creates new table with multiple named unique constraints", func() {
		q := NewQuery(nil, &CreateTableWithMultipleNamedUniques{})

		s := createTableQueryString(q, &CreateTableOptions{})
		Expect(s).To(Equal(`CREATE TABLE "create_table_with_multiple_named_uniques" ("id" bigserial, "account_id" bigint, "order_number" text, "store_order_number" text, PRIMARY KEY ("id"), UNIQUE ("account_id", "order_number"), UNIQUE ("account_id", "store_order_number"))`))
	})

	It("supports model without a table name", func() {
		type Model struct {
			tableName struct{} `pg:"_"`
			Id        int
		}

		q := NewQuery(nil, &Model{}).Table("dynamic_name")

		s := createTableQueryString(q, &CreateTableOptions{})
		Expect(s).To(Equal(`CREATE TABLE "dynamic_name" ("id" bigserial, PRIMARY KEY ("id"))`))
	})
})

func createTableQueryString(q *Query, opt *CreateTableOptions) string {
	qq := NewCreateTableQuery(q, opt)
	return queryString(qq)
}
