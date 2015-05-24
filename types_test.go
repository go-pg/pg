package pg_test

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"gopkg.in/pg.v3"
)

type JSONMap map[string]interface{}

func (m *JSONMap) Scan(value interface{}) error {
	return json.Unmarshal(value.([]byte), m)
}

func (m JSONMap) Value() (driver.Value, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

type JSONField struct {
	Foo string
}

type JSONRecord1 struct {
	Field JSONField
}

type JSONRecord2 struct {
	Field *JSONField
}

var (
	boolv   bool
	boolptr *bool

	stringv   string
	stringptr *string
	bytesv    []byte

	intv     int
	intvptr  *int
	int8v    int8
	int16v   int16
	int32v   int32
	int64v   int64
	uintv    uint
	uint8v   uint8
	uint16v  uint16
	uint32v  uint32
	uint64v  uint64
	uintptrv uintptr

	f32v float32
	f64v float64

	strSlice []string
	intSlice []int

	strStrMap map[string]string

	nullBool    sql.NullBool
	nullString  sql.NullString
	nullInt64   sql.NullInt64
	nullFloat64 sql.NullFloat64

	customStrSliceV customStrSlice

	timeptr *time.Time
	now     = time.Now()

	pgInts    pg.Ints
	pgStrings pg.Strings

	jsonMapPtr   *JSONMap
	jsonFieldPtr *JSONField
)

type conversionTest struct {
	src, dst, wanted interface{}
	pgtype           string

	wanterr  string
	wantnil  bool
	wantzero bool
}

var conversionTests = []conversionTest{
	{src: true, dst: nil, wanterr: "pg: Decode(nil)"},
	{src: true, dst: &uintptrv, wanterr: "pg: Decode(unsupported uintptr)"},
	{src: true, dst: boolv, wanterr: "pg: Decode(nonsettable bool)"},
	{src: true, dst: boolptr, wanterr: "pg: Decode(nonsettable *bool)"},

	{src: false, dst: &boolv, pgtype: "bool"},
	{src: true, dst: &boolv, pgtype: "bool"},
	{src: nil, dst: &boolv, pgtype: "bool", wantzero: true},
	{src: true, dst: &boolptr, pgtype: "bool"},
	{src: nil, dst: &boolptr, pgtype: "bool", wantnil: true},

	{src: "hello world", dst: &stringv, pgtype: "text"},
	{src: nil, dst: &stringv, pgtype: "text", wantzero: true},
	{src: "hello world", dst: &stringptr, pgtype: "text"},
	{src: nil, dst: &stringptr, pgtype: "text", wantnil: true},

	{src: []byte("hello world\000"), dst: &bytesv, pgtype: "bytea"},
	{src: []byte{}, dst: &bytesv, pgtype: "bytea", wantzero: true},
	{src: nil, dst: &bytesv, pgtype: "bytea", wantnil: true},

	{src: int(math.MaxInt32), dst: &intv, pgtype: "int"},
	{src: int(math.MinInt32), dst: &intv, pgtype: "int"},
	{src: nil, dst: &intv, pgtype: "int", wantzero: true},
	{src: int(math.MaxInt32), dst: &intvptr, pgtype: "int"},
	{src: nil, dst: &intvptr, pgtype: "int", wantnil: true},
	{src: int8(math.MaxInt8), dst: &int8v, pgtype: "smallint"},
	{src: int8(math.MinInt8), dst: &int8v, pgtype: "smallint"},
	{src: int16(math.MaxInt16), dst: &int16v, pgtype: "smallint"},
	{src: int16(math.MinInt16), dst: &int16v, pgtype: "smallint"},
	{src: int32(math.MaxInt32), dst: &int32v, pgtype: "int"},
	{src: int32(math.MinInt32), dst: &int32v, pgtype: "int"},
	{src: int64(math.MaxInt64), dst: &int64v, pgtype: "bigint"},
	{src: int64(math.MinInt64), dst: &int64v, pgtype: "bigint"},
	{src: uint(math.MaxUint32), dst: &uintv, pgtype: "bigint"},
	{src: uint8(math.MaxUint8), dst: &uint8v, pgtype: "smallint"},
	{src: uint16(math.MaxUint16), dst: &uint16v, pgtype: "int"},
	{src: uint32(math.MaxUint32), dst: &uint32v, pgtype: "bigint"},
	{src: uint64(math.MaxUint64), dst: &uint64v},

	{src: float32(math.MaxFloat32), dst: &f32v, pgtype: "decimal"},
	{src: float32(math.SmallestNonzeroFloat32), dst: &f32v, pgtype: "decimal"},
	{src: float64(math.MaxFloat64), dst: &f64v, pgtype: "decimal"},
	{src: float64(math.SmallestNonzeroFloat64), dst: &f64v, pgtype: "decimal"},

	{src: []string{"foo\n", "bar {}", "'\\\""}, dst: &strSlice, pgtype: "text[]"},
	{src: []string{}, dst: &strSlice, pgtype: "text[]", wantzero: true},
	{src: nil, dst: &strSlice, pgtype: "text[]", wantnil: true},

	{src: []int{}, dst: &intSlice, pgtype: "int[]"},
	{src: []int{1, 2, 3}, dst: &intSlice, pgtype: "int[]"},

	{
		src:    map[string]string{"foo\n =>": "bar\n =>", "'\\\"": "'\\\""},
		dst:    &strStrMap,
		pgtype: "hstore",
	},

	{src: &sql.NullBool{}, dst: &nullBool, pgtype: "bool"},
	{src: &sql.NullBool{Valid: true}, dst: &nullBool, pgtype: "bool"},
	{src: &sql.NullBool{Valid: true, Bool: true}, dst: &nullBool, pgtype: "bool"},

	{src: &sql.NullString{}, dst: &nullString, pgtype: "text"},
	{src: &sql.NullString{Valid: true}, dst: &nullString, pgtype: "text"},
	{src: &sql.NullString{Valid: true, String: "foo"}, dst: &nullString, pgtype: "text"},

	{src: &sql.NullInt64{}, dst: &nullInt64, pgtype: "bigint"},
	{src: &sql.NullInt64{Valid: true}, dst: &nullInt64, pgtype: "bigint"},
	{src: &sql.NullInt64{Valid: true, Int64: math.MaxInt64}, dst: &nullInt64, pgtype: "bigint"},

	{src: &sql.NullFloat64{}, dst: &nullFloat64, pgtype: "decimal"},
	{src: &sql.NullFloat64{Valid: true}, dst: &nullFloat64, pgtype: "decimal"},
	{src: &sql.NullFloat64{Valid: true, Float64: math.MaxFloat64}, dst: &nullFloat64, pgtype: "decimal"},

	{src: customStrSlice{}, dst: &customStrSliceV, wantzero: true},
	{src: nil, dst: &customStrSliceV, wantnil: true},
	{src: customStrSlice{"one", "two"}, dst: &customStrSliceV},

	{src: time.Time{}, dst: &time.Time{}, pgtype: "timestamp"},
	{src: time.Now(), dst: &time.Time{}, pgtype: "timestamp"},
	{src: time.Now().UTC(), dst: &time.Time{}, pgtype: "timestamp"},
	{src: nil, dst: &time.Time{}, pgtype: "timestamp", wantzero: true},
	{src: time.Now(), dst: &timeptr, pgtype: "timestamp"},
	{src: nil, dst: &timeptr, pgtype: "timestamp", wantnil: true},

	{src: time.Time{}, dst: &time.Time{}, pgtype: "timestamptz"},
	{src: time.Now(), dst: &time.Time{}, pgtype: "timestamptz"},
	{src: &now, dst: &time.Time{}, pgtype: "timestamptz"},
	{src: time.Now().UTC(), dst: &time.Time{}, pgtype: "timestamptz"},
	{src: nil, dst: &time.Time{}, pgtype: "timestamptz", wantzero: true},
	{src: time.Now(), dst: &timeptr, pgtype: "timestamptz"},
	{src: nil, dst: &timeptr, pgtype: "timestamptz", wantnil: true},

	{src: pg.Ints{1, 2, 3}, dst: &pgInts},
	{src: pg.Strings{"hello", "world"}, dst: &pgStrings},

	{src: JSONMap{"foo": "bar"}, dst: &JSONMap{}, pgtype: "json"},
	{src: JSONMap{"foo": "bar"}, dst: &jsonMapPtr, pgtype: "json"},
	{src: nil, dst: &jsonMapPtr, wantnil: true, pgtype: "json"},
	{src: `{"foo": "bar"}`, dst: &JSONField{}, wanted: JSONField{Foo: "bar"}},
	{src: `{"foo": "bar"}`, dst: &jsonFieldPtr, wanted: JSONField{Foo: "bar"}},
}

func deref(viface interface{}) interface{} {
	v := reflect.ValueOf(viface)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.IsValid() {
		return v.Interface()
	}
	return nil
}

func zero(v interface{}) interface{} {
	return reflect.Zero(reflect.ValueOf(v).Elem().Type()).Interface()
}

func (test *conversionTest) String() string {
	return fmt.Sprintf("src=%#v dst=%#v", test.src, test.dst)
}

func (test *conversionTest) Fatalf(t *testing.T, s interface{}, args ...interface{}) {
	args = append(args, test.String())
	t.Fatalf(fmt.Sprint(s)+" (%s)", args...)
}

func (test *conversionTest) Assert(t *testing.T, err error) {
	if test.wanterr != "" {
		if err == nil || err.Error() != test.wanterr {
			test.Fatalf(t, "error is %q, wanted %q", err, test.wanterr)
		}
		return
	}

	if err != nil {
		test.Fatalf(t, "error is %s, wanted nil", err)
	}

	src := deref(test.src)
	dst := deref(test.dst)

	if test.wantzero {
		dstValue := reflect.ValueOf(dst)
		if dstValue.Kind() == reflect.Slice {
			if dstValue.IsNil() {
				test.Fatalf(t, "got nil, wanted zero value")
			}
			if dstValue.Len() != 0 {
				test.Fatalf(t, "got %d items, wanted 0", dstValue.Len())
			}
		} else {
			zero := zero(test.dst)
			if dst != zero {
				test.Fatalf(t, "%#v != %#v", dst, zero)
			}
		}
		return
	}

	if test.wantnil {
		if dst == nil {
			return
		}
		if reflect.ValueOf(dst).IsNil() {
			return
		}
		test.Fatalf(t, "got %#v, wanted nil", dst)
		return
	}

	if dstTime, ok := dst.(time.Time); ok {
		srcTime := src.(time.Time)
		if dstTime.Unix() != srcTime.Unix() {
			test.Fatalf(t, "%#v != %#v", dstTime, srcTime)
		}
		return
	}

	wanted := test.wanted
	if wanted == nil {
		wanted = src
	}
	if !reflect.DeepEqual(dst, wanted) {
		test.Fatalf(t, "%#v != %#v", dst, wanted)
	}
}

func TestConversion(t *testing.T) {
	db := pgdb()
	db.Exec("CREATE EXTENSION hstore")
	defer db.Exec("DROP EXTENSION hstore")

	for _, test := range conversionTests {
		_, err := db.QueryOne(pg.LoadInto(test.dst), "SELECT (?) AS dst", test.src)
		test.Assert(t, err)
	}

	for _, test := range conversionTests {
		if test.pgtype == "" {
			continue
		}

		stmt, err := db.Prepare(fmt.Sprintf("SELECT ($1::%s) AS dst", test.pgtype))
		if err != nil {
			test.Fatalf(t, err)
		}

		_, err = stmt.QueryOne(pg.LoadInto(test.dst), test.src)
		test.Assert(t, err)

		if err := stmt.Close(); err != nil {
			test.Fatalf(t, err)
		}
	}

	for _, test := range conversionTests {
		fmt.Println(test)
		dst := struct{ Dst interface{} }{Dst: test.dst}
		_, err := db.QueryOne(&dst, "SELECT (?) AS dst", test.src)
		test.Assert(t, err)
	}

	for _, test := range conversionTests {
		if test.pgtype == "" {
			continue
		}

		stmt, err := db.Prepare(fmt.Sprintf("SELECT ($1::%s) AS dst", test.pgtype))
		if err != nil {
			test.Fatalf(t, err)
		}

		dst := struct{ Dst interface{} }{Dst: test.dst}
		_, err = stmt.QueryOne(&dst, test.src)
		test.Assert(t, err)

		if err := stmt.Close(); err != nil {
			test.Fatalf(t, err)
		}
	}
}
