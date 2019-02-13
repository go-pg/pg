package orm

import (
	"context"
	"reflect"
)

type hookStubs struct{}

func (hookStubs) AfterQuery(_ context.Context, _ DB) error {
	return nil
}

func (hookStubs) BeforeSelectQuery(_ context.Context, db DB, q *Query) (*Query, error) {
	return q, nil
}

func (hookStubs) AfterSelect(_ context.Context, _ DB) error {
	return nil
}

func (hookStubs) BeforeInsert(_ context.Context, _ DB) error {
	return nil
}

func (hookStubs) AfterInsert(_ context.Context, _ DB) error {
	return nil
}

func (hookStubs) BeforeUpdate(_ context.Context, _ DB) error {
	return nil
}

func (hookStubs) AfterUpdate(_ context.Context, _ DB) error {
	return nil
}

func (hookStubs) BeforeDelete(_ context.Context, _ DB) error {
	return nil
}

func (hookStubs) AfterDelete(_ context.Context, _ DB) error {
	return nil
}

func callHookSlice(
	slice reflect.Value,
	ptr bool,
	c context.Context,
	db DB,
	hook func(reflect.Value, context.Context, DB) error,
) error {
	var firstErr error
	for i := 0; i < slice.Len(); i++ {
		v := slice.Index(i)
		if !ptr {
			v = v.Addr()
		}

		err := hook(v, c, db)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

//------------------------------------------------------------------------------

type oldAfterQueryHook interface {
	AfterQuery(DB) error
}

var oldAfterQueryHookType = reflect.TypeOf((*oldAfterQueryHook)(nil)).Elem()

type afterQueryHook interface {
	AfterQuery(context.Context, DB) error
}

var afterQueryHookType = reflect.TypeOf((*afterQueryHook)(nil)).Elem()

func callAfterQueryHook(v reflect.Value, c context.Context, db DB) error {
	return v.Interface().(afterQueryHook).AfterQuery(c, db)
}

func callAfterQueryHookSlice(slice reflect.Value, ptr bool, c context.Context, db DB) error {
	return callHookSlice(slice, ptr, c, db, callAfterQueryHook)
}

//------------------------------------------------------------------------------

type oldBeforeSelectQueryHook interface {
	BeforeSelectQuery(DB, *Query) (*Query, error)
}

var oldBeforeSelectQueryHookType = reflect.TypeOf((*oldBeforeSelectQueryHook)(nil)).Elem()

type beforeSelectQueryHook interface {
	BeforeSelectQuery(context.Context, DB, *Query) (*Query, error)
}

var beforeSelectQueryHookType = reflect.TypeOf((*beforeSelectQueryHook)(nil)).Elem()

func callBeforeSelectQueryHook(v reflect.Value, c context.Context, db DB, q *Query) (*Query, error) {
	return v.Interface().(beforeSelectQueryHook).BeforeSelectQuery(c, db, q)
}

//------------------------------------------------------------------------------

type oldAfterSelectHook interface {
	AfterSelect(DB) error
}

var oldAfterSelectHookType = reflect.TypeOf((*oldAfterSelectHook)(nil)).Elem()

type afterSelectHook interface {
	AfterSelect(context.Context, DB) error
}

var afterSelectHookType = reflect.TypeOf((*afterSelectHook)(nil)).Elem()

func callAfterSelectHook(v reflect.Value, c context.Context, db DB) error {
	return v.Interface().(afterSelectHook).AfterSelect(c, db)
}

func callAfterSelectHookSlice(slice reflect.Value, ptr bool, c context.Context, db DB) error {
	return callHookSlice(slice, ptr, c, db, callAfterSelectHook)
}

//------------------------------------------------------------------------------

type oldBeforeInsertHook interface {
	BeforeInsert(DB) error
}

var oldBeforeInsertHookType = reflect.TypeOf((*oldBeforeInsertHook)(nil)).Elem()

type beforeInsertHook interface {
	BeforeInsert(context.Context, DB) error
}

var beforeInsertHookType = reflect.TypeOf((*beforeInsertHook)(nil)).Elem()

func callBeforeInsertHook(v reflect.Value, c context.Context, db DB) error {
	return v.Interface().(beforeInsertHook).BeforeInsert(c, db)
}

func callBeforeInsertHookSlice(slice reflect.Value, ptr bool, c context.Context, db DB) error {
	return callHookSlice(slice, ptr, c, db, callBeforeInsertHook)
}

//------------------------------------------------------------------------------

type oldAfterInsertHook interface {
	AfterInsert(DB) error
}

var oldAfterInsertHookType = reflect.TypeOf((*oldAfterInsertHook)(nil)).Elem()

type afterInsertHook interface {
	AfterInsert(context.Context, DB) error
}

var afterInsertHookType = reflect.TypeOf((*afterInsertHook)(nil)).Elem()

func callAfterInsertHook(v reflect.Value, c context.Context, db DB) error {
	return v.Interface().(afterInsertHook).AfterInsert(c, db)
}

func callAfterInsertHookSlice(slice reflect.Value, ptr bool, c context.Context, db DB) error {
	return callHookSlice(slice, ptr, c, db, callAfterInsertHook)
}

//------------------------------------------------------------------------------

type oldBeforeUpdateHook interface {
	BeforeUpdate(DB) error
}

var oldBeforeUpdateHookType = reflect.TypeOf((*oldBeforeUpdateHook)(nil)).Elem()

type beforeUpdateHook interface {
	BeforeUpdate(context.Context, DB) error
}

var beforeUpdateHookType = reflect.TypeOf((*beforeUpdateHook)(nil)).Elem()

func callBeforeUpdateHook(v reflect.Value, c context.Context, db DB) error {
	return v.Interface().(beforeUpdateHook).BeforeUpdate(c, db)
}

func callBeforeUpdateHookSlice(slice reflect.Value, ptr bool, c context.Context, db DB) error {
	return callHookSlice(slice, ptr, c, db, callBeforeUpdateHook)
}

//------------------------------------------------------------------------------

type oldAfterUpdateHook interface {
	AfterUpdate(DB) error
}

var oldAfterUpdateHookType = reflect.TypeOf((*oldAfterUpdateHook)(nil)).Elem()

type afterUpdateHook interface {
	AfterUpdate(context.Context, DB) error
}

var afterUpdateHookType = reflect.TypeOf((*afterUpdateHook)(nil)).Elem()

func callAfterUpdateHook(v reflect.Value, c context.Context, db DB) error {
	return v.Interface().(afterUpdateHook).AfterUpdate(c, db)
}

func callAfterUpdateHookSlice(slice reflect.Value, ptr bool, c context.Context, db DB) error {
	return callHookSlice(slice, ptr, c, db, callAfterUpdateHook)
}

//------------------------------------------------------------------------------

type oldBeforeDeleteHook interface {
	BeforeDelete(DB) error
}

var oldBeforeDeleteHookType = reflect.TypeOf((*oldBeforeDeleteHook)(nil)).Elem()

type beforeDeleteHook interface {
	BeforeDelete(context.Context, DB) error
}

var beforeDeleteHookType = reflect.TypeOf((*beforeDeleteHook)(nil)).Elem()

func callBeforeDeleteHook(v reflect.Value, c context.Context, db DB) error {
	return v.Interface().(beforeDeleteHook).BeforeDelete(c, db)
}

func callBeforeDeleteHookSlice(slice reflect.Value, ptr bool, c context.Context, db DB) error {
	return callHookSlice(slice, ptr, c, db, callBeforeDeleteHook)
}

//------------------------------------------------------------------------------

type oldAfterDeleteHook interface {
	AfterDelete(DB) error
}

var oldAfterDeleteHookType = reflect.TypeOf((*oldAfterDeleteHook)(nil)).Elem()

type afterDeleteHook interface {
	AfterDelete(context.Context, DB) error
}

var afterDeleteHookType = reflect.TypeOf((*afterDeleteHook)(nil)).Elem()

func callAfterDeleteHook(v reflect.Value, c context.Context, db DB) error {
	return v.Interface().(afterDeleteHook).AfterDelete(c, db)
}

func callAfterDeleteHookSlice(slice reflect.Value, ptr bool, c context.Context, db DB) error {
	return callHookSlice(slice, ptr, c, db, callAfterDeleteHook)
}
