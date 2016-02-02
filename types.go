package pg

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"sync"

	"gopkg.in/pg.v3/pgutil"
)

const (
	nullEmpty = 1 << 0
)

var (
	appenderType     = reflect.TypeOf(new(QueryAppender)).Elem()
	scannerType      = reflect.TypeOf(new(sql.Scanner)).Elem()
	driverValuerType = reflect.TypeOf(new(driver.Valuer)).Elem()
)

var structs = newStructCache()

func appendIfaceValue(dst []byte, v reflect.Value, quote bool) []byte {
	return appendIface(dst, v.Interface(), quote)
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

	return appendIfaceValue
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

type field struct {
	Name   string
	PGName string
	index  []int
	flags  int8

	appender valueAppender
	decoder  valueDecoder
}

func (f *field) Is(flag int8) bool {
	return f.flags&flag != 0
}

func (f *field) IsEmpty(v reflect.Value) bool {
	fv := v.FieldByIndex(f.index)
	return isEmptyValue(fv)
}

func (f *field) AppendValue(dst []byte, v reflect.Value, quote bool) []byte {
	fv := v.FieldByIndex(f.index)
	if f.Is(nullEmpty) && isEmptyValue(fv) {
		return appendNull(dst, quote)
	}
	return f.appender(dst, fv, quote)
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

func (f *field) DecodeValue(v reflect.Value, b []byte) error {
	v = fieldByIndex(v, f.index)
	if b == nil {
		return decodeNullValue(v)
	}
	return f.decoder(v, b)
}

//------------------------------------------------------------------------------

type structCache struct {
	fields    map[reflect.Type]fields
	fieldsMtx sync.RWMutex

	methods    map[reflect.Type]map[string]*method
	methodsMtx sync.RWMutex
}

func newStructCache() *structCache {
	return &structCache{
		fields:  make(map[reflect.Type]fields),
		methods: make(map[reflect.Type]map[string]*method),
	}
}

func (c *structCache) Fields(typ reflect.Type) fields {
	c.fieldsMtx.RLock()
	fs, ok := c.fields[typ]
	c.fieldsMtx.RUnlock()
	if ok {
		return fs
	}

	c.fieldsMtx.Lock()
	fs, ok = c.fields[typ]
	if !ok {
		fs = getFields(typ)
		c.fields[typ] = fs
	}
	c.fieldsMtx.Unlock()

	return fs
}

func (c *structCache) Methods(typ reflect.Type) map[string]*method {
	c.methodsMtx.RLock()
	ms, ok := c.methods[typ]
	c.methodsMtx.RUnlock()
	if ok {
		return ms
	}

	c.methodsMtx.Lock()
	ms, ok = c.methods[typ]
	if !ok {
		ms = getMethods(typ)
		c.methods[typ] = ms
	}
	c.methodsMtx.Unlock()

	return ms
}

//------------------------------------------------------------------------------

type fields struct {
	List  []*field
	Table map[string]*field
}

func newFields(numField int) fields {
	return fields{
		List:  make([]*field, 0, numField),
		Table: make(map[string]*field, numField),
	}
}

func (fs fields) Len() int {
	return len(fs.List)
}

func (fs *fields) Add(field *field) {
	fs.List = append(fs.List, field)
	fs.Table[field.Name] = field
}

func getFields(typ reflect.Type) fields {
	num := typ.NumField()
	fs := newFields(num)

	for i := 0; i < num; i++ {
		f := typ.Field(i)

		if f.Anonymous {
			typ := f.Type
			if typ.Kind() == reflect.Ptr {
				typ = typ.Elem()
			}
			for _, ff := range getFields(typ).List {
				ff.index = append(f.Index, ff.index...)
				fs.Add(ff)
			}
			continue
		}

		if f.PkgPath != "" && !f.Anonymous {
			continue
		}

		name, opts := parseTag(f.Tag.Get("pg"))
		if name == "-" {
			continue
		}
		if name == "" {
			name = pgutil.Underscore(f.Name)
		}

		fieldType := indirectType(f.Type)
		if fieldType.Kind() == reflect.Struct {
			for _, ff := range getFields(fieldType).List {
				ff.PGName = name + "." + ff.Name
				ff.Name = name + "__" + ff.Name
				ff.index = append(f.Index, ff.index...)
				fs.Add(ff)
			}
		}

		var flags int8
		if opts.Contains("nullempty") {
			flags |= nullEmpty
		}

		field := &field{
			Name:  name,
			index: f.Index,
			flags: flags,

			appender: getAppender(fieldType),
			decoder:  getDecoder(fieldType),
		}
		fs.Add(field)
	}

	return fs
}

//------------------------------------------------------------------------------

type method struct {
	Index int

	appender valueAppender
}

func (m *method) AppendValue(dst []byte, v reflect.Value, quote bool) []byte {
	mv := v.Method(m.Index).Call(nil)[0]
	return m.appender(dst, mv, quote)
}

func getMethods(typ reflect.Type) map[string]*method {
	num := typ.NumMethod()
	methods := make(map[string]*method, num)
	for i := 0; i < num; i++ {
		m := typ.Method(i)
		if m.Type.NumIn() > 1 {
			continue
		}
		if m.Type.NumOut() != 1 {
			continue
		}
		method := &method{
			Index: m.Index,

			appender: getAppender(m.Type.Out(0)),
		}
		methods[m.Name] = method
	}
	return methods
}

//------------------------------------------------------------------------------

func indirectType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}
