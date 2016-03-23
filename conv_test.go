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

	"gopkg.in/pg.v4"
	"gopkg.in/pg.v4/orm"
	"gopkg.in/pg.v4/types"
)

type JSONMap map[string]interface{}

func (m *JSONMap) Scan(b interface{}) error {
	if b == nil {
		*m = nil
		return nil
	}
	return json.Unmarshal(b.([]byte), m)
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

type (
	StringSlice  []string
	IntSlice     []int
	Int64Slice   []int64
	Float64Slice []float64
)

type Struct struct {
	Foo string
}

type conversionTest struct {
	i                int
	src, dst, wanted interface{}
	pgtype           string

	wanterr  string
	wantnil  bool
	wantzero bool
}

func unwrap(v interface{}) interface{} {
	if arr, ok := v.(*types.Array); ok {
		return arr.Value()
	}
	return v
}

func deref(vi interface{}) interface{} {
	v := reflect.ValueOf(vi)
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
	return fmt.Sprintf("#%d src=%#v dst=%#v", test.i, test.src, test.dst)
}

func (test *conversionTest) Assert(t *testing.T, err error) {
	if test.wanterr != "" {
		if err == nil || err.Error() != test.wanterr {
			t.Fatalf("got error %q, wanted %q", err, test.wanterr)
		}
		return
	}

	if err != nil {
		t.Fatalf("got error %q, wanted nil (%s)", err, test)
	}

	dst := reflect.Indirect(reflect.ValueOf(unwrap(test.dst))).Interface()

	if test.wantnil {
		dstValue := reflect.ValueOf(dst)
		if !dstValue.IsValid() {
			return
		}
		if dstValue.IsNil() {
			return
		}
		t.Fatalf("got %#v, wanted nil", dst)
		return
	}

	// Remove any intermediate pointers to compare values.
	dst = deref(unwrap(dst))
	src := deref(unwrap(test.src))

	if test.wantzero {
		dstValue := reflect.ValueOf(dst)
		switch dstValue.Kind() {
		case reflect.Slice, reflect.Map:
			if dstValue.IsNil() {
				t.Fatalf("got nil, wanted zero value")
			}
			if dstValue.Len() != 0 {
				t.Fatalf("got %d items, wanted 0", dstValue.Len())
			}
		default:
			zero := zero(test.dst)
			if dst != zero {
				t.Fatalf("%#v != %#v (%s)", dst, zero, test)
			}
		}
		return
	}

	if dstTime, ok := dst.(time.Time); ok {
		srcTime := src.(time.Time)
		if dstTime.Unix() != srcTime.Unix() {
			t.Fatalf("%#v != %#v", dstTime, srcTime)
		}
		return
	}

	if dstTimes, ok := dst.([]time.Time); ok {
		srcTimes := src.([]time.Time)
		for i, dstTime := range dstTimes {
			srcTime := srcTimes[i]
			if dstTime.Unix() != srcTime.Unix() {
				t.Fatalf("%#v != %#v", dstTime, srcTime)
			}
		}
		return
	}

	wanted := test.wanted
	if wanted == nil {
		wanted = src
	}
	if !reflect.DeepEqual(dst, wanted) {
		t.Fatalf("%#v != %#v (%s)", dst, wanted, test)
	}
}

func conversionTests() []conversionTest {
	return []conversionTest{
		{src: true, dst: nil, wanterr: "pg: Decode(nil)"},
		{src: true, dst: new(uintptr), wanterr: "pg: Decode(unsupported uintptr)"},
		{src: true, dst: true, wanterr: "pg: Decode(nonsettable bool)"},
		{src: true, dst: (*bool)(nil), wanterr: "pg: Decode(nonsettable *bool)"},

		{src: nil, dst: new(*bool), pgtype: "bool", wantnil: true},
		{src: nil, dst: new(bool), pgtype: "bool", wantzero: true},
		{src: true, dst: new(bool), pgtype: "bool"},
		{src: true, dst: new(*bool), pgtype: "bool"},

		{src: nil, dst: new(string), pgtype: "text", wantzero: true},
		{src: nil, dst: new(*string), pgtype: "text", wantnil: true},
		{src: "hello world", dst: new(string), pgtype: "text"},
		{src: "hello world", dst: new(*string), pgtype: "text"},
		{src: "'\"\000", dst: new(string), wanted: `'"`, pgtype: "text"},

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

		{src: nil, dst: new([]int), pgtype: "jsonb", wantnil: true},
		{src: []int(nil), dst: new([]int), pgtype: "jsonb", wantnil: true},
		{src: []int{}, dst: new([]int), pgtype: "jsonb", wantzero: true},
		{src: []int{1, 2, 3}, dst: new([]int), pgtype: "jsonb"},
		{src: IntSlice{1, 2, 3}, dst: new(IntSlice), pgtype: "jsonb"},

		{
			src:     nil,
			dst:     pg.Array([]int(nil)),
			pgtype:  "int[]",
			wantnil: true,
		},
		{
			src:     pg.Array([]int(nil)),
			dst:     pg.Array(new([]int)),
			pgtype:  "int[]",
			wantnil: true,
		},
		{
			src:    pg.Array([]int{}),
			dst:    pg.Array(new([]int)),
			pgtype: "int[]",
		},
		{
			src:    pg.Array([]int{1, 2, 3}),
			dst:    pg.Array(new([]int)),
			pgtype: "int[]",
		},

		{src: nil, dst: new([]int64), pgtype: "jsonb", wantnil: true},
		{src: []int64(nil), dst: new([]int64), pgtype: "jsonb", wantnil: true},
		{src: []int64{}, dst: new([]int64), pgtype: "jsonb", wantzero: true},
		{src: []int64{1, 2, 3}, dst: new([]int64), pgtype: "jsonb"},
		{src: Int64Slice{1, 2, 3}, dst: new(Int64Slice), pgtype: "jsonb"},

		{
			src:     nil,
			dst:     pg.Array(new([]int64)),
			pgtype:  "bigint[]",
			wantnil: true,
		},
		{
			src:     pg.Array([]int64(nil)),
			dst:     pg.Array(new([]int64)),
			pgtype:  "bigint[]",
			wantnil: true,
		},
		{
			src:    pg.Array([]int64{}),
			dst:    pg.Array(new([]int64)),
			pgtype: "bigint[]",
		},
		{
			src:    pg.Array([]int64{1, 2, 3}),
			dst:    pg.Array(new([]int64)),
			pgtype: "bigint[]",
		},

		{src: nil, dst: new([]float64), pgtype: "jsonb", wantnil: true},
		{src: []float64(nil), dst: new([]float64), pgtype: "jsonb", wantnil: true},
		{src: []float64{1.1, 2.22, 3.333}, dst: new([]float64), pgtype: "jsonb"},
		{src: Float64Slice{1.1, 2.22, 3.333}, dst: new(Float64Slice), pgtype: "jsonb"},

		{
			src:     nil,
			dst:     pg.Array(new([]float64)),
			pgtype:  "decimal[]",
			wantnil: true,
		},
		{
			src:     pg.Array([]float64(nil)),
			dst:     pg.Array(new([]float64)),
			pgtype:  "decimal[]",
			wantnil: true,
		},
		{
			src:    pg.Array([]float64{}),
			dst:    pg.Array(new([]float64)),
			pgtype: "decimal[]",
		},
		{
			src:    pg.Array([]float64{1.1, 2.22, 3.333}),
			dst:    pg.Array(new([]float64)),
			pgtype: "decimal[]",
		},

		{src: nil, dst: new([]string), pgtype: "jsonb", wantnil: true},
		{src: []string(nil), dst: new([]string), pgtype: "jsonb", wantnil: true},
		{src: []string{}, dst: new([]string), pgtype: "jsonb", wantzero: true},
		{src: []string{"foo\n", "bar {}", "'\\\""}, dst: new([]string), pgtype: "jsonb"},
		{src: StringSlice{"foo", "bar"}, dst: new(StringSlice), pgtype: "jsonb"},

		{
			src:     nil,
			dst:     pg.Array(new([]string)),
			pgtype:  "text[]",
			wantnil: true,
		},
		{
			src:     pg.Array([]string(nil)),
			dst:     pg.Array(new([]string)),
			pgtype:  "text[]",
			wantnil: true,
		},
		{
			src:    pg.Array([]string{}),
			dst:    pg.Array(new([]string)),
			pgtype: "text[]",
		},
		{
			src:    pg.Array([]string{"one", "two", "three"}),
			dst:    pg.Array(new([]string)),
			pgtype: "text[]",
		},

		{
			src:     nil,
			dst:     new(map[string]string),
			pgtype:  "jsonb",
			wantnil: true,
		},
		{
			src:     map[string]string(nil),
			dst:     new(map[string]string),
			pgtype:  "jsonb",
			wantnil: true,
		},
		{
			src:    map[string]string{"foo\n =>": "bar\n =>", "'\\\"": "'\\\""},
			dst:    new(map[string]string),
			pgtype: "jsonb",
		},

		{src: nil, dst: new(*sql.NullBool), pgtype: "bool", wantnil: true},
		{src: nil, dst: &sql.NullBool{}, pgtype: "bool", wanted: sql.NullBool{}},
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

		{src: nil, dst: &customStrSlice{}, wantnil: true},
		{src: customStrSlice{}, dst: &customStrSlice{}, wantzero: true},
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

		{
			src:     nil,
			dst:     pg.Array(new([]time.Time)),
			pgtype:  "timestamptz[]",
			wantnil: true,
		},
		{
			src:     pg.Array([]time.Time(nil)),
			dst:     pg.Array(new([]time.Time)),
			pgtype:  "timestamptz[]",
			wantnil: true,
		},
		{
			src:    pg.Array([]time.Time{}),
			dst:    pg.Array(new([]time.Time)),
			pgtype: "timestamptz[]",
		},
		{
			src:    pg.Array([]time.Time{time.Now(), time.Now(), time.Now()}),
			dst:    pg.Array(new([]time.Time)),
			pgtype: "timestamptz[]",
		},

		{src: 1, dst: new(pg.Ints), wanted: pg.Ints{1}},
		{src: "hello", dst: new(pg.Strings), wanted: pg.Strings{"hello"}},
		{src: 1, dst: new(pg.IntSet), wanted: pg.IntSet{1: struct{}{}}},

		{src: nil, dst: new(*JSONMap), wantnil: true, pgtype: "json"},
		{src: nil, dst: new(JSONMap), wantnil: true, pgtype: "json"},
		{src: JSONMap{}, dst: &JSONMap{}, pgtype: "json"},
		{src: JSONMap{"foo": "bar"}, dst: &JSONMap{}, pgtype: "json"},
		{src: JSONMap{"foo": "bar"}, dst: new(*JSONMap), pgtype: "json"},
		{src: `{"foo": "bar"}`, dst: &JSONField{}, wanted: JSONField{Foo: "bar"}},

		{src: nil, dst: new(*Struct), wantnil: true, pgtype: "json"},
		{src: nil, dst: new(Struct), wantzero: true, pgtype: "json"},
		{src: Struct{}, dst: &Struct{}, pgtype: "json"},
		{src: Struct{Foo: "bar"}, dst: &Struct{}, pgtype: "json"},
		{src: Struct{Foo: "bar"}, dst: new(*Struct), pgtype: "json"},
		{src: `{"foo": "bar"}`, dst: new(Struct), wanted: Struct{Foo: "bar"}},
	}
}

func TestConversion(t *testing.T) {
	db := pg.Connect(pgOptions())
	db.Exec("CREATE EXTENSION hstore")
	defer db.Exec("DROP EXTENSION hstore")

	for i, test := range conversionTests() {
		test.i = i

		var err error
		if _, ok := test.dst.(orm.ColumnScanner); ok {
			_, err = db.QueryOne(test.dst, "SELECT (?) AS dst", test.src)
		} else {
			dst := struct{ Dst interface{} }{Dst: test.dst}
			_, err = db.QueryOne(&dst, "SELECT (?) AS dst", test.src)
		}
		test.Assert(t, err)
	}

	for i, test := range conversionTests() {
		test.i = i

		if test.pgtype == "" {
			continue
		}

		stmt, err := db.Prepare(fmt.Sprintf("SELECT ($1::%s) AS dst", test.pgtype))
		if err != nil {
			t.Fatal(err)
		}

		if _, ok := test.dst.(orm.ColumnScanner); ok {
			_, err = stmt.QueryOne(test.dst, test.src)
		} else {
			dst := struct{ Dst interface{} }{Dst: test.dst}
			_, err = stmt.QueryOne(&dst, test.src)
		}
		test.Assert(t, err)

		if err := stmt.Close(); err != nil {
			t.Fatal(err)
		}
	}

	for i, test := range conversionTests() {
		test.i = i

		if _, ok := test.dst.(orm.ColumnScanner); ok {
			continue
		}

		_, err := db.QueryOne(pg.Scan(test.dst), "SELECT (?) AS dst", test.src)
		test.Assert(t, err)
	}

	for i, test := range conversionTests() {
		test.i = i

		if _, ok := test.dst.(orm.ColumnScanner); ok {
			continue
		}
		if test.pgtype == "" {
			continue
		}

		stmt, err := db.Prepare(fmt.Sprintf("SELECT ($1::%s) AS dst", test.pgtype))
		if err != nil {
			t.Fatal(err)
		}

		_, err = stmt.QueryOne(pg.Scan(test.dst), test.src)
		test.Assert(t, err)

		if err := stmt.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
