package orm

import (
	"context"
	"reflect"
)

type hookStubs struct{}

var _ AfterSelectHook = (*hookStubs)(nil)
var _ BeforeInsertHook = (*hookStubs)(nil)
var _ AfterInsertHook = (*hookStubs)(nil)
var _ BeforeUpdateHook = (*hookStubs)(nil)
var _ AfterUpdateHook = (*hookStubs)(nil)
var _ BeforeDeleteHook = (*hookStubs)(nil)
var _ AfterDeleteHook = (*hookStubs)(nil)

func (hookStubs) AfterSelect(c context.Context) error {
	return nil
}

func (hookStubs) BeforeInsert(c context.Context) (context.Context, error) {
	return c, nil
}

func (hookStubs) AfterInsert(c context.Context) error {
	return nil
}

func (hookStubs) BeforeUpdate(c context.Context) (context.Context, error) {
	return c, nil
}

func (hookStubs) AfterUpdate(c context.Context) error {
	return nil
}

func (hookStubs) BeforeDelete(c context.Context) (context.Context, error) {
	return c, nil
}

func (hookStubs) AfterDelete(c context.Context) error {
	return nil
}

func callHookSlice(
	c context.Context,
	slice reflect.Value,
	ptr bool,
	hook func(context.Context, reflect.Value) (context.Context, error),
) (context.Context, error) {
	var firstErr error
	for i := 0; i < slice.Len(); i++ {
		v := slice.Index(i)
		if !ptr {
			v = v.Addr()
		}

		var err error
		c, err = hook(c, v)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return c, firstErr
}

func callHookSlice2(
	c context.Context,
	slice reflect.Value,
	ptr bool,
	hook func(context.Context, reflect.Value) error,
) error {
	var firstErr error
	for i := 0; i < slice.Len(); i++ {
		v := slice.Index(i)
		if !ptr {
			v = v.Addr()
		}

		err := hook(c, v)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

//------------------------------------------------------------------------------

type BeforeScanHook interface {
	BeforeScan(context.Context) error
}

var beforeScanHookType = reflect.TypeOf((*BeforeScanHook)(nil)).Elem()

func callBeforeScanHook(c context.Context, v reflect.Value) error {
	return v.Interface().(BeforeScanHook).BeforeScan(c)
}

//------------------------------------------------------------------------------

type AfterScanHook interface {
	AfterScan(context.Context) error
}

var afterScanHookType = reflect.TypeOf((*AfterScanHook)(nil)).Elem()

func callAfterScanHook(c context.Context, v reflect.Value) error {
	return v.Interface().(AfterScanHook).AfterScan(c)
}

//------------------------------------------------------------------------------

type AfterSelectHook interface {
	AfterSelect(context.Context) error
}

var afterSelectHookType = reflect.TypeOf((*AfterSelectHook)(nil)).Elem()

func callAfterSelectHook(c context.Context, v reflect.Value) error {
	return v.Interface().(AfterSelectHook).AfterSelect(c)
}

func callAfterSelectHookSlice(
	c context.Context, slice reflect.Value, ptr bool,
) error {
	return callHookSlice2(c, slice, ptr, callAfterSelectHook)
}

//------------------------------------------------------------------------------

type BeforeInsertHook interface {
	BeforeInsert(context.Context) (context.Context, error)
}

var beforeInsertHookType = reflect.TypeOf((*BeforeInsertHook)(nil)).Elem()

func callBeforeInsertHook(c context.Context, v reflect.Value) (context.Context, error) {
	return v.Interface().(BeforeInsertHook).BeforeInsert(c)
}

func callBeforeInsertHookSlice(
	c context.Context, slice reflect.Value, ptr bool,
) (context.Context, error) {
	return callHookSlice(c, slice, ptr, callBeforeInsertHook)
}

//------------------------------------------------------------------------------

type AfterInsertHook interface {
	AfterInsert(context.Context) error
}

var afterInsertHookType = reflect.TypeOf((*AfterInsertHook)(nil)).Elem()

func callAfterInsertHook(c context.Context, v reflect.Value) error {
	return v.Interface().(AfterInsertHook).AfterInsert(c)
}

func callAfterInsertHookSlice(
	c context.Context, slice reflect.Value, ptr bool,
) error {
	return callHookSlice2(c, slice, ptr, callAfterInsertHook)
}

//------------------------------------------------------------------------------

type BeforeUpdateHook interface {
	BeforeUpdate(context.Context) (context.Context, error)
}

var beforeUpdateHookType = reflect.TypeOf((*BeforeUpdateHook)(nil)).Elem()

func callBeforeUpdateHook(c context.Context, v reflect.Value) (context.Context, error) {
	return v.Interface().(BeforeUpdateHook).BeforeUpdate(c)
}

func callBeforeUpdateHookSlice(
	c context.Context, slice reflect.Value, ptr bool,
) (context.Context, error) {
	return callHookSlice(c, slice, ptr, callBeforeUpdateHook)
}

//------------------------------------------------------------------------------

type AfterUpdateHook interface {
	AfterUpdate(context.Context) error
}

var afterUpdateHookType = reflect.TypeOf((*AfterUpdateHook)(nil)).Elem()

func callAfterUpdateHook(c context.Context, v reflect.Value) error {
	return v.Interface().(AfterUpdateHook).AfterUpdate(c)
}

func callAfterUpdateHookSlice(
	c context.Context, slice reflect.Value, ptr bool,
) error {
	return callHookSlice2(c, slice, ptr, callAfterUpdateHook)
}

//------------------------------------------------------------------------------

type BeforeDeleteHook interface {
	BeforeDelete(context.Context) (context.Context, error)
}

var beforeDeleteHookType = reflect.TypeOf((*BeforeDeleteHook)(nil)).Elem()

func callBeforeDeleteHook(c context.Context, v reflect.Value) (context.Context, error) {
	return v.Interface().(BeforeDeleteHook).BeforeDelete(c)
}

func callBeforeDeleteHookSlice(
	c context.Context, slice reflect.Value, ptr bool,
) (context.Context, error) {
	return callHookSlice(c, slice, ptr, callBeforeDeleteHook)
}

//------------------------------------------------------------------------------

type AfterDeleteHook interface {
	AfterDelete(context.Context) error
}

var afterDeleteHookType = reflect.TypeOf((*AfterDeleteHook)(nil)).Elem()

func callAfterDeleteHook(c context.Context, v reflect.Value) error {
	return v.Interface().(AfterDeleteHook).AfterDelete(c)
}

func callAfterDeleteHookSlice(
	c context.Context, slice reflect.Value, ptr bool,
) error {
	return callHookSlice2(c, slice, ptr, callAfterDeleteHook)
}

//------------------------------------------------------------------------------

var oldHooks = []reflect.Type{
	oldAfterQueryHookType,
	oldAfterQueryHookType2,
	oldBeforeSelectQueryHookType,
	oldBeforeSelectQueryHookType2,
	oldAfterSelectHookType,
	oldAfterSelectHookType2,
	oldBeforeInsertHookType,
	oldBeforeInsertHookType2,
	oldAfterInsertHookType,
	oldAfterInsertHookType2,
	oldBeforeUpdateHookType,
	oldBeforeUpdateHookType2,
	oldAfterUpdateHookType,
	oldAfterUpdateHookType2,
	oldBeforeDeleteHookType,
	oldBeforeDeleteHookType2,
	oldAfterDeleteHookType,
	oldAfterDeleteHookType2,
}

//------------------------------------------------------------------------------

type oldAfterQueryHook interface {
	AfterQuery(DB) error
}

var oldAfterQueryHookType = reflect.TypeOf((*oldAfterQueryHook)(nil)).Elem()

type oldAfterQueryHook2 interface {
	AfterQuery(context.Context, DB) error
}

var oldAfterQueryHookType2 = reflect.TypeOf((*oldAfterQueryHook2)(nil)).Elem()

//------------------------------------------------------------------------------

type oldBeforeSelectQueryHook interface {
	BeforeSelectQuery(DB, *Query) (*Query, error)
}

var oldBeforeSelectQueryHookType = reflect.TypeOf((*oldBeforeSelectQueryHook)(nil)).Elem()

type oldBeforeSelectQueryHook2 interface {
	BeforeSelectQuery(context.Context, DB, *Query) (*Query, error)
}

var oldBeforeSelectQueryHookType2 = reflect.TypeOf((*oldBeforeSelectQueryHook2)(nil)).Elem()

//------------------------------------------------------------------------------

type oldAfterSelectHook interface {
	AfterSelect(DB) error
}

var oldAfterSelectHookType = reflect.TypeOf((*oldAfterSelectHook)(nil)).Elem()

type oldAfterSelectHook2 interface {
	AfterSelect(context.Context, DB) error
}

var oldAfterSelectHookType2 = reflect.TypeOf((*oldAfterSelectHook2)(nil)).Elem()

//------------------------------------------------------------------------------

type oldBeforeInsertHook interface {
	BeforeInsert(DB) error
}

var oldBeforeInsertHookType = reflect.TypeOf((*oldBeforeInsertHook)(nil)).Elem()

type oldBeforeInsertHook2 interface {
	BeforeInsert(context.Context, DB) error
}

var oldBeforeInsertHookType2 = reflect.TypeOf((*oldBeforeInsertHook2)(nil)).Elem()

//------------------------------------------------------------------------------

type oldAfterInsertHook interface {
	AfterInsert(DB) error
}

var oldAfterInsertHookType = reflect.TypeOf((*oldAfterInsertHook)(nil)).Elem()

type oldAfterInsertHook2 interface {
	AfterInsert(context.Context, DB) error
}

var oldAfterInsertHookType2 = reflect.TypeOf((*oldAfterInsertHook2)(nil)).Elem()

//------------------------------------------------------------------------------

type oldBeforeUpdateHook interface {
	BeforeUpdate(DB) error
}

var oldBeforeUpdateHookType = reflect.TypeOf((*oldBeforeUpdateHook)(nil)).Elem()

type oldBeforeUpdateHook2 interface {
	BeforeUpdate(context.Context, DB) error
}

var oldBeforeUpdateHookType2 = reflect.TypeOf((*oldBeforeUpdateHook2)(nil)).Elem()

//------------------------------------------------------------------------------

type oldAfterUpdateHook interface {
	AfterUpdate(DB) error
}

var oldAfterUpdateHookType = reflect.TypeOf((*oldAfterUpdateHook)(nil)).Elem()

type oldAfterUpdateHook2 interface {
	AfterUpdate(context.Context, DB) error
}

var oldAfterUpdateHookType2 = reflect.TypeOf((*oldAfterUpdateHook2)(nil)).Elem()

//------------------------------------------------------------------------------

type oldBeforeDeleteHook interface {
	BeforeDelete(DB) error
}

var oldBeforeDeleteHookType = reflect.TypeOf((*oldBeforeDeleteHook)(nil)).Elem()

type oldBeforeDeleteHook2 interface {
	BeforeDelete(context.Context, DB) error
}

var oldBeforeDeleteHookType2 = reflect.TypeOf((*oldBeforeDeleteHook2)(nil)).Elem()

//------------------------------------------------------------------------------

type oldAfterDeleteHook interface {
	AfterDelete(DB) error
}

var oldAfterDeleteHookType = reflect.TypeOf((*oldAfterDeleteHook)(nil)).Elem()

type oldAfterDeleteHook2 interface {
	AfterDelete(context.Context, DB) error
}

var oldAfterDeleteHookType2 = reflect.TypeOf((*oldAfterDeleteHook2)(nil)).Elem()
