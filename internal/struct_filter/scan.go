package struct_filter

import (
	"database/sql"
	"reflect"
	"strconv"
	"time"

	"github.com/go-pg/pg/types"
)

var timeType = reflect.TypeOf((*time.Time)(nil)).Elem()
var durationType = reflect.TypeOf((*time.Duration)(nil)).Elem()
var nullBoolType = reflect.TypeOf((*sql.NullBool)(nil)).Elem()
var nullInt64Type = reflect.TypeOf((*sql.NullInt64)(nil)).Elem()
var nullFloat64Type = reflect.TypeOf((*sql.NullFloat64)(nil)).Elem()
var nullStringType = reflect.TypeOf((*sql.NullString)(nil)).Elem()

type ScanFunc func(v reflect.Value, values []string) error

func scanner(typ reflect.Type) ScanFunc {
	switch typ {
	case timeType:
		return scanTime
	case durationType:
		return scanDuration
	case nullBoolType:
		return scanNullBool
	case nullInt64Type:
		return scanNullInt64
	case nullFloat64Type:
		return scanNullFloat64
	case nullStringType:
		return scanNullString
	}

	switch typ.Kind() {
	case reflect.Bool:
		return scanBool
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return scanInt64
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return scanUint64
	case reflect.Float32:
		return scanFloat32
	case reflect.Float64:
		return scanFloat64
	case reflect.String:
		return scanString
	}
	return nil
}

func arrayScanner(typ reflect.Type) ScanFunc {
	switch typ.Elem().Kind() {
	case reflect.Int:
		return scanIntSlice
	case reflect.Int32:
		return scanInt32Slice
	case reflect.Int64:
		return scanInt64Slice
	case reflect.String:
		return scanStringSlice
	}
	return nil
}

func scanBool(v reflect.Value, values []string) error {
	f, err := strconv.ParseBool(values[0])
	if err != nil {
		return err
	}
	v.SetBool(f)
	return nil
}

func scanInt64(v reflect.Value, values []string) error {
	n, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil {
		return err
	}
	v.SetInt(n)
	return nil
}

func scanUint64(v reflect.Value, values []string) error {
	n, err := strconv.ParseUint(values[0], 10, 64)
	if err != nil {
		return err
	}
	v.SetUint(n)
	return nil
}

func scanFloat32(v reflect.Value, values []string) error {
	return scanFloat(v, values, 32)
}

func scanFloat64(v reflect.Value, values []string) error {
	return scanFloat(v, values, 64)
}

func scanFloat(v reflect.Value, values []string, bits int) error {
	n, err := strconv.ParseFloat(values[0], bits)
	if err != nil {
		return err
	}
	v.SetFloat(n)
	return nil
}

func scanString(v reflect.Value, values []string) error {
	v.SetString(values[0])
	return nil
}

func scanTime(v reflect.Value, values []string) error {
	tm, err := ParseTime(values[0])
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(tm))
	return nil
}

func ParseTime(s string) (time.Time, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return time.Unix(n, 0), nil
	}
	return types.ParseTimeString(s)
}

func scanDuration(v reflect.Value, values []string) error {
	dur, err := time.ParseDuration(values[0])
	if err != nil {
		return err
	}
	v.SetInt(int64(dur))
	return nil
}

func scanNullBool(v reflect.Value, values []string) error {
	value := sql.NullBool{
		Valid: true,
	}

	s := values[0]
	if s == "" {
		v.Set(reflect.ValueOf(value))
		return nil
	}

	f, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}

	value.Bool = f
	v.Set(reflect.ValueOf(value))

	return nil
}

func scanNullInt64(v reflect.Value, values []string) error {
	value := sql.NullInt64{
		Valid: true,
	}

	s := values[0]
	if s == "" {
		v.Set(reflect.ValueOf(value))
		return nil
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return err
	}

	value.Int64 = n
	v.Set(reflect.ValueOf(value))

	return nil
}

func scanNullFloat64(v reflect.Value, values []string) error {
	value := sql.NullFloat64{
		Valid: true,
	}

	s := values[0]
	if s == "" {
		v.Set(reflect.ValueOf(value))
		return nil
	}

	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}

	value.Float64 = n
	v.Set(reflect.ValueOf(value))

	return nil
}

func scanNullString(v reflect.Value, values []string) error {
	value := sql.NullString{
		Valid: true,
	}

	s := values[0]
	if s == "" {
		v.Set(reflect.ValueOf(value))
		return nil
	}

	value.String = s
	v.Set(reflect.ValueOf(value))

	return nil
}

func scanIntSlice(v reflect.Value, values []string) error {
	nn := make([]int, 0, len(values))
	for _, s := range values {
		n, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		nn = append(nn, n)
	}
	v.Set(reflect.ValueOf(nn))
	return nil
}

func scanInt32Slice(v reflect.Value, values []string) error {
	nn := make([]int32, 0, len(values))
	for _, s := range values {
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return err
		}
		nn = append(nn, int32(n))
	}
	v.Set(reflect.ValueOf(nn))
	return nil
}

func scanInt64Slice(v reflect.Value, values []string) error {
	nn := make([]int64, 0, len(values))
	for _, s := range values {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		nn = append(nn, n)
	}
	v.Set(reflect.ValueOf(nn))
	return nil
}

func scanStringSlice(v reflect.Value, values []string) error {
	v.Set(reflect.ValueOf(values))
	return nil
}
