package pg

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/pg.v3/pgutil"
)

const (
	appenderFlag     = 1
	sqlScannerFlag   = 2
	driverValuerFlag = 4
)

var (
	appenderType     = reflect.TypeOf(new(Appender)).Elem()
	sqlScannerType   = reflect.TypeOf(new(sql.Scanner)).Elem()
	driverValuerType = reflect.TypeOf(new(driver.Valuer)).Elem()
)

var structs = newStructCache()

type valueConstructor func(*baseValue) valuer

var valueConstructors = [...]valueConstructor{
	reflect.Bool:          newBoolValue,
	reflect.Int:           newIntValue,
	reflect.Int8:          newIntValue,
	reflect.Int16:         newIntValue,
	reflect.Int32:         newIntValue,
	reflect.Int64:         newIntValue,
	reflect.Uint:          newUintValue,
	reflect.Uint8:         newUintValue,
	reflect.Uint16:        newUintValue,
	reflect.Uint32:        newUintValue,
	reflect.Uint64:        newUintValue,
	reflect.Uintptr:       nil,
	reflect.Float32:       newFloatValue,
	reflect.Float64:       newFloatValue,
	reflect.Complex64:     nil,
	reflect.Complex128:    nil,
	reflect.Array:         nil,
	reflect.Chan:          nil,
	reflect.Func:          nil,
	reflect.Interface:     nil,
	reflect.Map:           nil,
	reflect.Ptr:           nil,
	reflect.Slice:         nil,
	reflect.String:        newStringValue,
	reflect.Struct:        nil,
	reflect.UnsafePointer: nil,
}

type valuer interface {
	Source() interface{}
	Type() reflect.Type
	Index() []int
	AppendValue([]byte, reflect.Value) []byte
	DecodeValue(reflect.Value, []byte) error
}

func newValue(src interface{}, index []int) valuer {
	var typ reflect.Type
	switch v := src.(type) {
	case reflect.StructField:
		typ = v.Type
	case reflect.Method:
		typ = v.Type.Out(0)
	default:
		panic("not reached")
	}

	bv := &baseValue{
		src:   src,
		typ:   typ,
		index: index,
	}

	if typ.Implements(appenderType) {
		bv.flags |= appenderFlag
	}
	if typ.Implements(sqlScannerType) {
		bv.flags |= sqlScannerFlag
	}
	if typ.Implements(driverValuerType) {
		bv.flags |= driverValuerFlag
	}
	if bv.flags != 0 {
		return bv
	}

	switch typ {
	case timeType:
		return &timeValue{
			baseValue: bv,
		}
	}

	if constructor := valueConstructors[typ.Kind()]; constructor != nil {
		return constructor(bv)
	}
	return bv
}

type baseValue struct {
	src   interface{}
	typ   reflect.Type
	index []int

	flags int
}

func (f *baseValue) Source() interface{} {
	return f.src
}

func (f *baseValue) getValue(v reflect.Value) reflect.Value {
	switch f.src.(type) {
	case reflect.StructField:
		return v.FieldByIndex(f.index)
	case reflect.Method:
		return v.Method(f.index[0]).Call(nil)[0]
	default:
		panic("not reached")
	}
}

func (f *baseValue) Type() reflect.Type {
	return f.typ
}

func (f *baseValue) Index() []int {
	return f.index
}

func (f *baseValue) AppendValue(dst []byte, v reflect.Value) []byte {
	fv := f.getValue(v)
	if f.flags&appenderFlag != 0 {
		return fv.Interface().(Appender).Append(dst)
	}
	if f.flags&driverValuerFlag != 0 {
		valuer := fv.Interface().(driver.Valuer)
		return appendDriverValue(dst, valuer)
	}
	return appendIface(dst, fv.Interface())
}

func (f *baseValue) DecodeValue(v reflect.Value, b []byte) error {
	fv := f.getValue(v)
	if f.flags&sqlScannerFlag != 0 {
		scanner := fv.Interface().(sql.Scanner)
		return decodeScanner(scanner, b)
	}
	return DecodeValue(fv.Addr(), b)
}

type boolValue struct {
	*baseValue
}

func newBoolValue(bv *baseValue) valuer {
	return &boolValue{
		baseValue: bv,
	}
}

func (f *boolValue) AppendValue(dst []byte, v reflect.Value) []byte {
	fv := f.getValue(v)
	return appendBool(dst, fv.Bool())
}

func (f *boolValue) DecodeValue(v reflect.Value, b []byte) error {
	fv := f.getValue(v)
	return decodeBoolValue(fv, b)
}

type intValue struct {
	*baseValue
}

func newIntValue(bv *baseValue) valuer {
	return &intValue{
		baseValue: bv,
	}
}

func (f *intValue) AppendValue(dst []byte, v reflect.Value) []byte {
	fv := f.getValue(v)
	return strconv.AppendInt(dst, fv.Int(), 10)
}

func (f *intValue) DecodeValue(v reflect.Value, b []byte) error {
	return decodeIntValue(f.getValue(v), b)
}

type uintValue struct {
	*baseValue
}

func newUintValue(bv *baseValue) valuer {
	return &uintValue{
		baseValue: bv,
	}
}

func (f *uintValue) AppendValue(dst []byte, v reflect.Value) []byte {
	fv := f.getValue(v)
	return strconv.AppendUint(dst, fv.Uint(), 10)
}

func (f *uintValue) DecodeValue(v reflect.Value, b []byte) error {
	return decodeUintValue(f.getValue(v), b)
}

type floatValue struct {
	*baseValue
}

func newFloatValue(bv *baseValue) valuer {
	return &floatValue{
		baseValue: bv,
	}
}

func (f *floatValue) AppendValue(dst []byte, v reflect.Value) []byte {
	fv := f.getValue(v)
	return appendFloat(dst, fv.Float())
}

func (f *floatValue) DecodeValue(v reflect.Value, b []byte) error {
	return decodeFloatValue(f.getValue(v), b)
}

type stringValue struct {
	*baseValue
}

func newStringValue(bv *baseValue) valuer {
	return &stringValue{
		baseValue: bv,
	}
}

func (f *stringValue) AppendValue(dst []byte, v reflect.Value) []byte {
	fv := f.getValue(v)
	return appendString(dst, fv.String())
}

func (f *stringValue) DecodeValue(v reflect.Value, b []byte) error {
	fv := f.getValue(v)
	return decodeStringValue(fv, b)
}

type timeValue struct {
	*baseValue
}

func (f *timeValue) AppendValue(dst []byte, v reflect.Value) []byte {
	fv := f.getValue(v)
	return appendTime(dst, fv.Interface().(time.Time))
}

func (f *timeValue) DecodeValue(v reflect.Value, b []byte) error {
	return decodeTimeValue(f.getValue(v), b)
}

//------------------------------------------------------------------------------

type structCache struct {
	fields    map[reflect.Type]map[string]valuer
	fieldsMtx sync.RWMutex

	methods    map[reflect.Type]map[string]valuer
	methodsMtx sync.RWMutex
}

func newStructCache() *structCache {
	return &structCache{
		fields:  make(map[reflect.Type]map[string]valuer),
		methods: make(map[reflect.Type]map[string]valuer),
	}
}

func (c *structCache) Fields(typ reflect.Type) map[string]valuer {
	c.fieldsMtx.RLock()
	fs, ok := c.fields[typ]
	c.fieldsMtx.RUnlock()
	if ok {
		return fs
	}

	c.fieldsMtx.Lock()
	fs, ok = c.fields[typ]
	if !ok {
		fs = fields(typ)
		c.fields[typ] = fs
	}
	c.fieldsMtx.Unlock()

	return fs
}

func (c *structCache) Methods(typ reflect.Type) map[string]valuer {
	c.methodsMtx.RLock()
	ms, ok := c.methods[typ]
	c.methodsMtx.RUnlock()
	if ok {
		return ms
	}

	c.methodsMtx.Lock()
	ms, ok = c.methods[typ]
	if !ok {
		ms = methods(typ)
		c.methods[typ] = ms
	}
	c.methodsMtx.Unlock()

	return ms
}

func fields(typ reflect.Type) map[string]valuer {
	num := typ.NumField()
	dst := make(map[string]valuer, num)
	for i := 0; i < num; i++ {
		f := typ.Field(i)

		if f.Anonymous {
			typ := f.Type
			if typ.Kind() == reflect.Ptr {
				typ = typ.Elem()
			}
			for name, ff := range fields(typ) {
				dst[name] = newValue(ff.Source(), append(f.Index, ff.Index()...))
			}
			continue
		}

		if f.PkgPath != "" {
			continue
		}

		tokens := strings.Split(f.Tag.Get("pg"), ",")
		name := tokens[0]
		if name == "-" {
			continue
		}
		if name == "" {
			name = pgutil.Underscore(f.Name)
		}

		tt := indirectType(f.Type)
		if tt.Kind() == reflect.Struct {
			for subname, ff := range fields(tt) {
				dst[name+"__"+subname] = newValue(ff.Source(), append(f.Index, ff.Index()...))
			}
		}

		dst[name] = newValue(f, f.Index)
	}
	return dst
}

func methods(typ reflect.Type) map[string]valuer {
	num := typ.NumMethod()
	methods := make(map[string]valuer, num)
	for i := 0; i < num; i++ {
		m := typ.Method(i)
		if m.Type.NumIn() > 1 {
			continue
		}
		if m.Type.NumOut() != 1 {
			continue
		}
		methods[m.Name] = newValue(m, []int{m.Index})
	}
	return methods
}

func indirectType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}
