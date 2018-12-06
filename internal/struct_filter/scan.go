package struct_filter

import (
	"reflect"
	"strconv"
	"time"

	"github.com/go-pg/pg/types"
)

var timeType = reflect.TypeOf((*time.Time)(nil)).Elem()
var durationType = reflect.TypeOf((*time.Duration)(nil)).Elem()

type ScanFunc func(v reflect.Value, values []string) error

func scanner(typ reflect.Type) ScanFunc {
	switch typ {
	case timeType:
		return scanTime
	case durationType:
		return scanDuration
	}

	switch typ.Kind() {
	case reflect.Bool:
		return scanBool
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return scanInt64
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return scanUint64
	case reflect.Float32, reflect.Float64:
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

func scanFloat64(v reflect.Value, values []string) error {
	n, err := strconv.ParseFloat(values[0], 64)
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
	tm, err := types.ParseTimeString(values[0])
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(tm))
	return nil
}

func scanDuration(v reflect.Value, values []string) error {
	dur, err := time.ParseDuration(values[0])
	if err != nil {
		return err
	}
	v.SetInt(int64(dur))
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

func scanStringSlice(v reflect.Value, values []string) error {
	v.Set(reflect.ValueOf(values))
	return nil
}
