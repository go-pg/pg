package orm

import "reflect"

const (
	AfterQueryHookFlag = 1 << iota
	AfterSelectHookFlag
	BeforeCreateHookFlag
	AfterCreateHookFlag
)

func callHookSlice(slice reflect.Value, ptr bool, db DB, hook func(reflect.Value, DB) error) error {
	var retErr error
	for i := 0; i < slice.Len(); i++ {
		var err error
		if ptr {
			err = hook(slice.Index(i), db)
		} else {
			err = hook(slice.Index(i).Addr(), db)
		}
		if err != nil && retErr == nil {
			retErr = err
		}
	}
	return retErr
}

type afterQueryHook interface {
	AfterQuery(db DB) error
}

var afterQueryHookType = reflect.TypeOf((*afterQueryHook)(nil)).Elem()

func callAfterQueryHook(v reflect.Value, db DB) error {
	return v.Interface().(afterQueryHook).AfterQuery(db)
}

func callAfterQueryHookSlice(slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(slice, ptr, db, callAfterQueryHook)
}

type afterSelectHook interface {
	AfterSelect(db DB) error
}

var afterSelectHookType = reflect.TypeOf((*afterSelectHook)(nil)).Elem()

func callAfterSelectHook(v reflect.Value, db DB) error {
	return v.Interface().(afterSelectHook).AfterSelect(db)
}

func callAfterSelectHookSlice(slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(slice, ptr, db, callAfterSelectHook)
}

type beforeCreateHook interface {
	BeforeCreate(db DB) error
}

var beforeCreateHookType = reflect.TypeOf((*beforeCreateHook)(nil)).Elem()

func callBeforeCreateHook(v reflect.Value, db DB) error {
	return v.Interface().(beforeCreateHook).BeforeCreate(db)
}

func callBeforeCreateHookSlice(slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(slice, ptr, db, callBeforeCreateHook)
}

type afterCreateHook interface {
	AfterCreate(db DB) error
}

var afterCreateHookType = reflect.TypeOf((*afterCreateHook)(nil)).Elem()

func callAfterCreateHook(v reflect.Value, db DB) error {
	return v.Interface().(afterCreateHook).AfterCreate(db)
}

func callAfterCreateHookSlice(slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(slice, ptr, db, callAfterCreateHook)
}
