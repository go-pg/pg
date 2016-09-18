package orm

import "reflect"

const (
	AfterQueryHookFlag = 1 << iota
	AfterSelectHookFlag
	BeforeInsertHookFlag
	AfterInsertHookFlag
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

type beforeInsertHook interface {
	BeforeInsert(db DB) error
}

var beforeInsertHookType = reflect.TypeOf((*beforeInsertHook)(nil)).Elem()

func callBeforeInsertHook(v reflect.Value, db DB) error {
	return v.Interface().(beforeInsertHook).BeforeInsert(db)
}

func callBeforeInsertHookSlice(slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(slice, ptr, db, callBeforeInsertHook)
}

type afterInsertHook interface {
	AfterInsert(db DB) error
}

var afterInsertHookType = reflect.TypeOf((*afterInsertHook)(nil)).Elem()

func callAfterInsertHook(v reflect.Value, db DB) error {
	return v.Interface().(afterInsertHook).AfterInsert(db)
}

func callAfterInsertHookSlice(slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(slice, ptr, db, callAfterInsertHook)
}
