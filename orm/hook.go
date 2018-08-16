package orm

import (
	"reflect"
)

type hookStubs struct{}

func (hookStubs) AfterQuery(_ DB) error {
	return nil
}

func (hookStubs) BeforeSelectQuery(db DB, q *Query) (*Query, error) {
	return q, nil
}

func (hookStubs) AfterSelect(_ DB) error {
	return nil
}

func (hookStubs) BeforeInsert(_ DB) error {
	return nil
}

func (hookStubs) AfterInsert(_ DB) error {
	return nil
}

func (hookStubs) BeforeUpdate(_ DB) error {
	return nil
}

func (hookStubs) AfterUpdate(_ DB) error {
	return nil
}

func (hookStubs) BeforeDelete(_ DB) error {
	return nil
}

func (hookStubs) AfterDelete(_ DB) error {
	return nil
}

func callHookSlice(slice reflect.Value, ptr bool, db DB, hook func(reflect.Value, DB) error) error {
	var firstErr error
	for i := 0; i < slice.Len(); i++ {
		var err error
		if ptr {
			err = hook(slice.Index(i), db)
		} else {
			err = hook(slice.Index(i).Addr(), db)
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

//------------------------------------------------------------------------------

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

//------------------------------------------------------------------------------

type beforeSelectQueryHook interface {
	BeforeSelectQuery(db DB, q *Query) (*Query, error)
}

var beforeSelectQueryHookType = reflect.TypeOf((*beforeSelectQueryHook)(nil)).Elem()

func callBeforeSelectQueryHook(v reflect.Value, db DB, q *Query) (*Query, error) {
	return v.Interface().(beforeSelectQueryHook).BeforeSelectQuery(db, q)
}

//------------------------------------------------------------------------------

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

//------------------------------------------------------------------------------

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

//------------------------------------------------------------------------------

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

//------------------------------------------------------------------------------

type beforeUpdateHook interface {
	BeforeUpdate(db DB) error
}

var beforeUpdateHookType = reflect.TypeOf((*beforeUpdateHook)(nil)).Elem()

func callBeforeUpdateHook(v reflect.Value, db DB) error {
	return v.Interface().(beforeUpdateHook).BeforeUpdate(db)
}

func callBeforeUpdateHookSlice(slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(slice, ptr, db, callBeforeUpdateHook)
}

//------------------------------------------------------------------------------

type afterUpdateHook interface {
	AfterUpdate(db DB) error
}

var afterUpdateHookType = reflect.TypeOf((*afterUpdateHook)(nil)).Elem()

func callAfterUpdateHook(v reflect.Value, db DB) error {
	return v.Interface().(afterUpdateHook).AfterUpdate(db)
}

func callAfterUpdateHookSlice(slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(slice, ptr, db, callAfterUpdateHook)
}

//------------------------------------------------------------------------------

type beforeDeleteHook interface {
	BeforeDelete(db DB) error
}

var beforeDeleteHookType = reflect.TypeOf((*beforeDeleteHook)(nil)).Elem()

func callBeforeDeleteHook(v reflect.Value, db DB) error {
	return v.Interface().(beforeDeleteHook).BeforeDelete(db)
}

func callBeforeDeleteHookSlice(slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(slice, ptr, db, callBeforeDeleteHook)
}

//------------------------------------------------------------------------------

type afterDeleteHook interface {
	AfterDelete(db DB) error
}

var afterDeleteHookType = reflect.TypeOf((*afterDeleteHook)(nil)).Elem()

func callAfterDeleteHook(v reflect.Value, db DB) error {
	return v.Interface().(afterDeleteHook).AfterDelete(db)
}

func callAfterDeleteHookSlice(slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(slice, ptr, db, callAfterDeleteHook)
}
