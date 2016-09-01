package orm

import (
	"context"
	"fmt"
	"reflect"
)

const (
	BeforeCreateHookFlag = 1 << iota
	AfterCreateHookFlag
)

type beforeCreateHook interface {
	BeforeCreate(context.Context) error
}

var beforeCreateHookType = reflect.TypeOf((*beforeCreateHook)(nil)).Elem()

type afterCreateHook interface {
	AfterCreate(context.Context) error
}

var afterCreateHookType = reflect.TypeOf((*afterCreateHook)(nil)).Elem()

func callBeforeCreateHook(c context.Context, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		return v.Interface().(beforeCreateHook).BeforeCreate(c)
	case reflect.Struct:
		return v.Addr().Interface().(beforeCreateHook).BeforeCreate(c)
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if err := callBeforeCreateHook(c, v.Index(i)); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("pg: Model(unsupported %s)", v.Type())
}

func callAfterCreateHook(c context.Context, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		return v.Interface().(afterCreateHook).AfterCreate(c)
	case reflect.Struct:
		return v.Addr().Interface().(afterCreateHook).AfterCreate(c)
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if err := callAfterCreateHook(c, v.Index(i)); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("pg: Model(unsupported %s)", v.Type())
}
