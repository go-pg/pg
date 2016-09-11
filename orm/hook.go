package orm

import (
	"fmt"
	"reflect"
)

const (
	AfterSelectHookFlag = 1 << iota
	BeforeCreateHookFlag
	AfterCreateHookFlag
)

type afterSelectHook interface {
	AfterSelect(db DB) error
}

var afterSelectHookType = reflect.TypeOf((*afterSelectHook)(nil)).Elem()

type beforeCreateHook interface {
	BeforeCreate(db DB) error
}

var beforeCreateHookType = reflect.TypeOf((*beforeCreateHook)(nil)).Elem()

type afterCreateHook interface {
	AfterCreate(db DB) error
}

var afterCreateHookType = reflect.TypeOf((*afterCreateHook)(nil)).Elem()

func callAfterSelectHook(v reflect.Value, db DB) error {
	return v.Interface().(afterSelectHook).AfterSelect(db)
}

func callAfterSelectHookSlice(slice reflect.Value, ptr bool, db DB) error {
	var retErr error
	for i := 0; i < slice.Len(); i++ {
		var err error
		if ptr {
			err = callAfterSelectHook(slice.Index(i), db)
		} else {
			err = callAfterSelectHook(slice.Index(i).Addr(), db)
		}
		if err != nil && retErr == nil {
			retErr = err
		}
	}
	return retErr
}

func callBeforeCreateHook(v reflect.Value, db DB) error {
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		return v.Interface().(beforeCreateHook).BeforeCreate(db)
	case reflect.Struct:
		return v.Addr().Interface().(beforeCreateHook).BeforeCreate(db)
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if err := callBeforeCreateHook(v.Index(i), db); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("pg: Model(unsupported %s)", v.Type())
}

func callAfterCreateHook(v reflect.Value, db DB) error {
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		return v.Interface().(afterCreateHook).AfterCreate(db)
	case reflect.Struct:
		return v.Addr().Interface().(afterCreateHook).AfterCreate(db)
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if err := callAfterCreateHook(v.Index(i), db); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("pg: Model(unsupported %s)", v.Type())
}
