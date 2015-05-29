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

type conversionTest struct {
	src, dst, wanted interface{}
	pgtype           string

	wanterr  string
	wantnil  bool
	wantzero bool
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
	conversionTests := []conversionTest{
		{src: true, dst: nil, wanterr: "pg: Decode(nil)"},
		{src: true, dst: new(uintptr), wanterr: "pg: Decode(unsupported uintptr)"},
		{src: true, dst: true, wanterr: "pg: Decode(nonsettable bool)"},
		{src: true, dst: (*bool)(nil), wanterr: "pg: Decode(nonsettable *bool)"},

		{src: false, dst: new(bool), pgtype: "bool"},
		{src: true, dst: new(bool), pgtype: "bool"},
		{src: nil, dst: new(bool), pgtype: "bool", wantzero: true},
		{src: true, dst: new(*bool), pgtype: "bool"},
		{src: nil, dst: new(*bool), pgtype: "bool", wantnil: true},

		{src: "hello world", dst: new(string), pgtype: "text"},
		{src: nil, dst: new(string), pgtype: "text", wantzero: true},
		{src: "hello world", dst: new(*string), pgtype: "text"},
		{src: nil, dst: new(*string), pgtype: "text", wantnil: true},

		{src: []byte("hello world\000"), dst: new([]byte), pgtype: "bytea"},
		{src: []byte{}, dst: new([]byte), pgtype: "bytea", wantzero: true},
		{src: nil, dst: new([]byte), pgtype: "bytea", wantnil: true},

		{src: int(math.MaxInt32), dst: new(int), pgtype: "int"},
		{src: int(math.MinInt32), dst: new(int), pgtype: "int"},
		{src: nil, dst: new(int), pgtype: "int", wantzero: true},
		{src: int(math.MaxInt32), dst: new(*int), pgtype: "int"},
		{src: nil, dst: new(*int), pgtype: "int", wantnil: true},
		{src: int8(math.MaxInt8), dst: new(int8), pgtype: "smallint"},
		{src: int8(math.MinInt8), dst: new(int8), pgtype: "smallint"},
		{src: int16(math.MaxInt16), dst: new(int16), pgtype: "smallint"},
		{src: int16(math.MinInt16), dst: new(int16), pgtype: "smallint"},
		{src: int32(math.MaxInt32), dst: new(int32), pgtype: "int"},
		{src: int32(math.MinInt32), dst: new(int32), pgtype: "int"},
		{src: int64(math.MaxInt64), dst: new(int64), pgtype: "bigint"},
		{src: int64(math.MinInt64), dst: new(int64), pgtype: "bigint"},
		{src: uint(math.MaxUint32), dst: new(uint), pgtype: "bigint"},
		{src: uint8(math.MaxUint8), dst: new(uint8), pgtype: "smallint"},
		{src: uint16(math.MaxUint16), dst: new(uint16), pgtype: "int"},
		{src: uint32(math.MaxUint32), dst: new(uint32), pgtype: "bigint"},
		{src: uint64(math.MaxUint64), dst: new(uint64)},

		{src: float32(math.MaxFloat32), dst: new(float32), pgtype: "decimal"},
		{src: float32(math.SmallestNonzeroFloat32), dst: new(float32), pgtype: "decimal"},
		{src: float64(math.MaxFloat64), dst: new(float64), pgtype: "decimal"},
		{src: float64(math.SmallestNonzeroFloat64), dst: new(float64), pgtype: "decimal"},

		{src: []int{}, dst: new([]int), pgtype: "int[]"},
		{src: []int{1, 2, 3}, dst: new([]int), pgtype: "int[]"},
		{src: []int64{1, 2, 3}, dst: new([]int64), pgtype: "bigint[]"},
		{src: []float64{1.1, 2.22, 3.333}, dst: new([]float64), pgtype: "double precision[]"},

		{src: []string{"foo\n", "bar {}", "'\\\""}, dst: new([]string), pgtype: "text[]"},
		{src: []string{}, dst: new([]string), pgtype: "text[]", wantzero: true},
		{src: nil, dst: new([]string), pgtype: "text[]", wantnil: true},

		{
			src:    map[string]string{"foo\n =>": "bar\n =>", "'\\\"": "'\\\""},
			dst:    new(map[string]string),
			pgtype: "hstore",
		},

		{src: &sql.NullBool{}, dst: &sql.NullBool{}, pgtype: "bool"},
		{src: &sql.NullBool{Valid: true}, dst: &sql.NullBool{}, pgtype: "bool"},
		{src: &sql.NullBool{Valid: true, Bool: true}, dst: &sql.NullBool{}, pgtype: "bool"},

		{src: &sql.NullString{}, dst: &sql.NullString{}, pgtype: "text"},
		{src: &sql.NullString{Valid: true}, dst: &sql.NullString{}, pgtype: "text"},
		{src: &sql.NullString{Valid: true, String: "foo"}, dst: &sql.NullString{}, pgtype: "text"},

		{src: &sql.NullInt64{}, dst: &sql.NullInt64{}, pgtype: "bigint"},
		{src: &sql.NullInt64{Valid: true}, dst: &sql.NullInt64{}, pgtype: "bigint"},
		{src: &sql.NullInt64{Valid: true, Int64: math.MaxInt64}, dst: &sql.NullInt64{}, pgtype: "bigint"},

		{src: &sql.NullFloat64{}, dst: &sql.NullFloat64{}, pgtype: "decimal"},
		{src: &sql.NullFloat64{Valid: true}, dst: &sql.NullFloat64{}, pgtype: "decimal"},
		{src: &sql.NullFloat64{Valid: true, Float64: math.MaxFloat64}, dst: &sql.NullFloat64{}, pgtype: "decimal"},

		{src: customStrSlice{}, dst: &customStrSlice{}, wantzero: true},
		{src: nil, dst: &customStrSlice{}, wantnil: true},
		{src: customStrSlice{"one", "two"}, dst: &customStrSlice{}},

		{src: time.Time{}, dst: &time.Time{}, pgtype: "timestamp"},
		{src: time.Now(), dst: &time.Time{}, pgtype: "timestamp"},
		{src: time.Now().UTC(), dst: &time.Time{}, pgtype: "timestamp"},
		{src: nil, dst: &time.Time{}, pgtype: "timestamp", wantzero: true},
		{src: time.Now(), dst: new(*time.Time), pgtype: "timestamp"},
		{src: nil, dst: new(*time.Time), pgtype: "timestamp", wantnil: true},

		{src: time.Time{}, dst: &time.Time{}, pgtype: "timestamptz"},
		{src: time.Now(), dst: &time.Time{}, pgtype: "timestamptz"},
		{src: time.Now().UTC(), dst: &time.Time{}, pgtype: "timestamptz"},
		{src: nil, dst: &time.Time{}, pgtype: "timestamptz", wantzero: true},
		{src: time.Now(), dst: new(*time.Time), pgtype: "timestamptz"},
		{src: nil, dst: new(*time.Time), pgtype: "timestamptz", wantnil: true},

		{src: 1, dst: new(pg.Ints), wanted: pg.Ints{1}},
		{src: "hello", dst: new(pg.Strings), wanted: pg.Strings{"hello"}},
		{src: 1, dst: new(pg.IntSet), wanted: pg.IntSet{1: struct{}{}}},

		{src: JSONMap{"foo": "bar"}, dst: &JSONMap{}, pgtype: "json"},
		{src: JSONMap{"foo": "bar"}, dst: new(*JSONMap), pgtype: "json"},
		{src: nil, dst: new(*JSONMap), wantnil: true, pgtype: "json"},
		{src: `{"foo": "bar"}`, dst: &JSONField{}, wanted: JSONField{Foo: "bar"}},
		{src: `{"foo": "bar"}`, dst: new(*JSONField), wanted: JSONField{Foo: "bar"}},
	}

	db := pgdb()
	db.Exec("CREATE EXTENSION hstore")
	defer db.Exec("DROP EXTENSION hstore")

	for _, test := range conversionTests {
		var err error
		if _, ok := test.dst.(pg.ColumnLoader); ok {
			_, err = db.QueryOne(test.dst, "SELECT (?) AS dst", test.src)
		} else {
			dst := struct{ Dst interface{} }{Dst: test.dst}
			_, err = db.QueryOne(&dst, "SELECT (?) AS dst", test.src)
		}
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

		if _, ok := test.dst.(pg.ColumnLoader); ok {
			_, err = stmt.QueryOne(test.dst, test.src)
		} else {
			dst := struct{ Dst interface{} }{Dst: test.dst}
			_, err = stmt.QueryOne(&dst, test.src)
		}
		test.Assert(t, err)

		if err := stmt.Close(); err != nil {
			test.Fatalf(t, err)
		}
	}

	for _, test := range conversionTests {
		if _, ok := test.dst.(pg.ColumnLoader); ok {
			continue
		}

		_, err := db.QueryOne(pg.LoadInto(test.dst), "SELECT (?) AS dst", test.src)
		test.Assert(t, err)
	}

	for _, test := range conversionTests {
		if _, ok := test.dst.(pg.ColumnLoader); ok {
			continue
		}
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
}
