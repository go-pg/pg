package urlstruct

import (
	"context"
	"net/url"
	"reflect"
	"strings"
)

type structDecoder struct {
	v     reflect.Value
	sinfo *StructInfo

	decMap           map[string]*structDecoder
	paramUnmarshaler ParamUnmarshaler
}

func newStructDecoder(v reflect.Value) *structDecoder {
	v = reflect.Indirect(v)
	return &structDecoder{
		v:     v,
		sinfo: DescribeStruct(v.Type()),
	}
}

func (d *structDecoder) Decode(ctx context.Context, values url.Values) error {
	var maps map[string][]string

	for name, values := range values {
		name = strings.TrimPrefix(name, ":")
		name = strings.TrimSuffix(name, "[]")

		if name, key, ok := mapKey(name); ok {
			if mdec := d.mapDecoder(name); mdec != nil {
				if err := mdec.decodeParam(ctx, key, values); err != nil {
					return err
				}
				continue
			}

			if maps == nil {
				maps = make(map[string][]string)
			}
			maps[name] = append(maps[name], key, values[0])
			continue
		}

		if err := d.decodeParam(ctx, name, values); err != nil {
			return err
		}
	}

	for name, values := range maps {
		if err := d.decodeParam(ctx, name, values); err != nil {
			return nil
		}
	}

	for _, idx := range d.sinfo.unmarshalerIndexes {
		fv := d.v.FieldByIndex(idx)
		if fv.Kind() == reflect.Struct {
			fv = fv.Addr()
		} else if fv.IsNil() {
			fv.Set(reflect.New(fv.Type().Elem()))
		}

		u := fv.Interface().(Unmarshaler)
		if err := u.UnmarshalValues(ctx, values); err != nil {
			return err
		}
	}

	if d.sinfo.isUnmarshaler {
		return d.v.Addr().Interface().(Unmarshaler).UnmarshalValues(ctx, values)
	}

	return nil
}

func (d *structDecoder) mapDecoder(name string) *structDecoder {
	if dec, ok := d.decMap[name]; ok {
		return dec
	}
	if idx, ok := d.sinfo.structs[name]; ok {
		dec := newStructDecoder(d.v.FieldByIndex(idx))
		if d.decMap == nil {
			d.decMap = make(map[string]*structDecoder)
			d.decMap[name] = dec
		}
		return dec
	}
	return nil
}

func (d *structDecoder) decodeParam(ctx context.Context, name string, values []string) error {
	if field := d.sinfo.Field(name); field != nil && !field.noDecode {
		return field.scanValue(field.Value(d.v), values)
	}

	if d.sinfo.isParamUnmarshaler {
		if d.paramUnmarshaler == nil {
			d.paramUnmarshaler = d.v.Addr().Interface().(ParamUnmarshaler)
		}
		return d.paramUnmarshaler.UnmarshalParam(ctx, name, values)
	}

	return nil
}

func mapKey(s string) (name string, key string, ok bool) {
	ind := strings.IndexByte(s, '[')
	if ind == -1 || s[len(s)-1] != ']' {
		return "", "", false
	}
	key = s[ind+1 : len(s)-1]
	if key == "" {
		return "", "", false
	}
	name = s[:ind]
	return name, key, true
}
