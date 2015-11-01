package pg

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"sync"

	"gopkg.in/pg.v3/pgutil"
)

const (
	nullemptyFlag = 8
)

var (
	appenderType     = reflect.TypeOf(new(QueryAppender)).Elem()
	scannerType      = reflect.TypeOf(new(sql.Scanner)).Elem()
	driverValuerType = reflect.TypeOf(new(driver.Valuer)).Elem()
)

var structs = newStructCache()

func _appendValue(dst []byte, v reflect.Value) []byte {
	return appendIface(dst, v.Interface())
}

func getAppender(typ reflect.Type) valueAppender {
	switch typ {
	case timeType:
		return appendTimeValue
	}

	if typ.Implements(appenderType) {
		return appendAppenderValue
	}

	if typ.Implements(driverValuerType) {
		return appendDriverValuerValue
	}

	kind := typ.Kind()
	if appender := valueAppenders[kind]; appender != nil {
		return appender
	}

	return _appendValue
}

func getDecoder(typ reflect.Type) valueDecoder {
	switch typ {
	case timeType:
		return decodeTimeValue
	}

	if reflect.PtrTo(typ).Implements(scannerType) {
		return decodeScannerAddrValue
	}

	if typ.Implements(scannerType) {
		return decodeScannerValue
	}

	kind := typ.Kind()
	if dec := valueDecoders[kind]; dec != nil {
		return dec
	}

	return nil
}

type pgValue struct {
	Source interface{}
	Type   reflect.Type
	Index  []int

	NullEmpty bool

	appender valueAppender
	decoder  valueDecoder
}

func newPGValue(src interface{}, index []int) *pgValue {
	var typ reflect.Type
	switch v := src.(type) {
	case reflect.StructField:
		typ = v.Type
	case reflect.Method:
		typ = v.Type.Out(0)
	default:
		panic("not reached")
	}

	return &pgValue{
		Source: src,
		Type:   typ,
		Index:  index,

		appender: getAppender(typ),
		decoder:  getDecoder(typ),
	}
}

func (pgv *pgValue) AppendValue(dst []byte, v reflect.Value) []byte {
	fv := pgv.getValue(v)
	if pgv.NullEmpty && isEmptyValue(fv) {
		return appendNull(dst)
	}
	return pgv.appender(dst, fv)
}

func fieldByIndex(v reflect.Value, index []int) reflect.Value {
	if len(index) == 1 {
		return v.Field(index[0])
	}
	for i, x := range index {
		if i > 0 && v.Kind() == reflect.Ptr {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(x)
	}
	return v
}

func (pgv *pgValue) DecodeValue(v reflect.Value, b []byte) error {
	v = fieldByIndex(v, pgv.Index)
	if b == nil {
		return decodeNullValue(v)
	}
	return pgv.decoder(v, b)
}

func (pgv *pgValue) getValue(v reflect.Value) reflect.Value {
	switch pgv.Source.(type) {
	case reflect.StructField:
		return v.FieldByIndex(pgv.Index)
	case reflect.Method:
		return v.Method(pgv.Index[0]).Call(nil)[0]
	default:
		panic("not reached")
	}
}

//------------------------------------------------------------------------------

type structCache struct {
	fields    map[reflect.Type]map[string]*pgValue
	fieldsMtx sync.RWMutex

	methods    map[reflect.Type]map[string]*pgValue
	methodsMtx sync.RWMutex
}

func newStructCache() *structCache {
	return &structCache{
		fields:  make(map[reflect.Type]map[string]*pgValue),
		methods: make(map[reflect.Type]map[string]*pgValue),
	}
}

func (c *structCache) Fields(typ reflect.Type) map[string]*pgValue {
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

func (c *structCache) Methods(typ reflect.Type) map[string]*pgValue {
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

func fields(typ reflect.Type) map[string]*pgValue {
	num := typ.NumField()
	dst := make(map[string]*pgValue, num)
	for i := 0; i < num; i++ {
		f := typ.Field(i)

		if f.Anonymous {
			typ := f.Type
			if typ.Kind() == reflect.Ptr {
				typ = typ.Elem()
			}
			for name, ff := range fields(typ) {
				dst[name] = newPGValue(ff.Source, append(f.Index, ff.Index...))
			}
			continue
		}

		if f.PkgPath != "" {
			continue
		}

		name, opts := parseTag(f.Tag.Get("pg"))
		if name == "-" {
			continue
		}
		if name == "" {
			name = pgutil.Underscore(f.Name)
		}

		tt := indirectType(f.Type)
		if tt.Kind() == reflect.Struct {
			for subname, ff := range fields(tt) {
				dst[name+"__"+subname] = newPGValue(ff.Source, append(f.Index, ff.Index...))
			}
		}

		val := newPGValue(f, f.Index)
		if opts.Contains("nullempty") {
			val.NullEmpty = true
		}
		dst[name] = val
	}
	return dst
}

func methods(typ reflect.Type) map[string]*pgValue {
	num := typ.NumMethod()
	methods := make(map[string]*pgValue, num)
	for i := 0; i < num; i++ {
		m := typ.Method(i)
		if m.Type.NumIn() > 1 {
			continue
		}
		if m.Type.NumOut() != 1 {
			continue
		}
		methods[m.Name] = newPGValue(m, []int{m.Index})
	}
	return methods
}

func indirectType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}
