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

	pgv := &pgValue{
		Source: src,
		Type:   typ,
		Index:  index,
	}

	switch typ {
	case timeType:
		pgv.appender = appendTimeValue
		pgv.decoder = decodeTimeValue
		return pgv
	}

	kind := typ.Kind()

	if typ.Implements(appenderType) {
		pgv.appender = appendAppenderValue
	} else if typ.Implements(driverValuerType) {
		pgv.appender = appendDriverValuerValue
	} else if appender := valueAppenders[kind]; appender != nil {
		pgv.appender = appender
	}

	if reflect.PtrTo(typ).Implements(scannerType) {
		pgv.decoder = decodeScannerAddrValue
	} else if typ.Implements(scannerType) {
		pgv.decoder = decodeScannerValue
	} else if dec := valueDecoders[kind]; dec != nil {
		pgv.decoder = dec
	}

	return pgv
}

func (pgv *pgValue) AppendValue(dst []byte, v reflect.Value) []byte {
	fv := pgv.getValue(v)
	if pgv.NullEmpty && isEmptyValue(fv) {
		return appendNull(dst)
	}
	if pgv.appender != nil {
		return pgv.appender(dst, fv)
	}
	return appendIface(dst, fv.Interface())
}

func (pgv *pgValue) DecodeValue(v reflect.Value, b []byte) error {
	fv := pgv.getValue(v)
	if b == nil {
		return decodeNullValue(fv)
	}
	if pgv.decoder != nil {
		return pgv.decoder(fv, b)
	}
	return DecodeValue(fv.Addr(), b)
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
