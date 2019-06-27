package orm

import (
	"context"
	"reflect"
)

type hookStubs struct{}

var _ BeforeSelectHook = (*hookStubs)(nil)
var _ AfterSelectHook = (*hookStubs)(nil)
var _ BeforeInsertHook = (*hookStubs)(nil)
var _ AfterInsertHook = (*hookStubs)(nil)
var _ BeforeUpdateHook = (*hookStubs)(nil)
var _ AfterUpdateHook = (*hookStubs)(nil)
var _ BeforeDeleteHook = (*hookStubs)(nil)
var _ AfterDeleteHook = (*hookStubs)(nil)

func (hookStubs) BeforeSelect(q *Query) (*Query, error) {
	return q, nil
}

func (hookStubs) AfterSelect(q *Query) (*Query, error) {
	return q, nil
}

func (hookStubs) BeforeInsert(q *Query) (*Query, error) {
	return q, nil
}

func (hookStubs) AfterInsert(q *Query) (*Query, error) {
	return q, nil
}

func (hookStubs) BeforeUpdate(q *Query) (*Query, error) {
	return q, nil
}

func (hookStubs) AfterUpdate(q *Query) (*Query, error) {
	return q, nil
}

func (hookStubs) BeforeDelete(q *Query) (*Query, error) {
	return q, nil
}

func (hookStubs) AfterDelete(q *Query) (*Query, error) {
	return q, nil
}

func callHookSlice(
	slice reflect.Value,
	ptr bool,
	q *Query,
	hook func(reflect.Value, *Query) (*Query, error),
) (*Query, error) {
	var firstErr error
	for i := 0; i < slice.Len(); i++ {
		v := slice.Index(i)
		if !ptr {
			v = v.Addr()
		}

		var err error
		q, err = hook(v, q)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return q, firstErr
}

//------------------------------------------------------------------------------

type BeforeSelectHook interface {
	BeforeSelect(*Query) (*Query, error)
}

var beforeSelectHookType = reflect.TypeOf((*BeforeSelectHook)(nil)).Elem()

func callBeforeSelectHook(v reflect.Value, q *Query) (*Query, error) {
	return v.Interface().(BeforeSelectHook).BeforeSelect(q)
}

//------------------------------------------------------------------------------

type AfterSelectHook interface {
	AfterSelect(*Query) (*Query, error)
}

var afterSelectHookType = reflect.TypeOf((*AfterSelectHook)(nil)).Elem()

func callAfterSelectHook(v reflect.Value, q *Query) (*Query, error) {
	return v.Interface().(AfterSelectHook).AfterSelect(q)
}

func callAfterSelectHookSlice(slice reflect.Value, ptr bool, q *Query) (*Query, error) {
	return callHookSlice(slice, ptr, q, callAfterSelectHook)
}

//------------------------------------------------------------------------------

type BeforeInsertHook interface {
	BeforeInsert(*Query) (*Query, error)
}

var beforeInsertHookType = reflect.TypeOf((*BeforeInsertHook)(nil)).Elem()

func callBeforeInsertHook(v reflect.Value, q *Query) (*Query, error) {
	return v.Interface().(BeforeInsertHook).BeforeInsert(q)
}

func callBeforeInsertHookSlice(slice reflect.Value, ptr bool, q *Query) (*Query, error) {
	return callHookSlice(slice, ptr, q, callBeforeInsertHook)
}

//------------------------------------------------------------------------------

type AfterInsertHook interface {
	AfterInsert(*Query) (*Query, error)
}

var afterInsertHookType = reflect.TypeOf((*AfterInsertHook)(nil)).Elem()

func callAfterInsertHook(v reflect.Value, q *Query) (*Query, error) {
	return v.Interface().(AfterInsertHook).AfterInsert(q)
}

func callAfterInsertHookSlice(slice reflect.Value, ptr bool, q *Query) (*Query, error) {
	return callHookSlice(slice, ptr, q, callAfterInsertHook)
}

//------------------------------------------------------------------------------

type BeforeUpdateHook interface {
	BeforeUpdate(*Query) (*Query, error)
}

var beforeUpdateHookType = reflect.TypeOf((*BeforeUpdateHook)(nil)).Elem()

func callBeforeUpdateHook(v reflect.Value, q *Query) (*Query, error) {
	return v.Interface().(BeforeUpdateHook).BeforeUpdate(q)
}

func callBeforeUpdateHookSlice(slice reflect.Value, ptr bool, q *Query) (*Query, error) {
	return callHookSlice(slice, ptr, q, callBeforeUpdateHook)
}

//------------------------------------------------------------------------------

type AfterUpdateHook interface {
	AfterUpdate(*Query) (*Query, error)
}

var afterUpdateHookType = reflect.TypeOf((*AfterUpdateHook)(nil)).Elem()

func callAfterUpdateHook(v reflect.Value, q *Query) (*Query, error) {
	return v.Interface().(AfterUpdateHook).AfterUpdate(q)
}

func callAfterUpdateHookSlice(slice reflect.Value, ptr bool, q *Query) (*Query, error) {
	return callHookSlice(slice, ptr, q, callAfterUpdateHook)
}

//------------------------------------------------------------------------------

type BeforeDeleteHook interface {
	BeforeDelete(*Query) (*Query, error)
}

var beforeDeleteHookType = reflect.TypeOf((*BeforeDeleteHook)(nil)).Elem()

func callBeforeDeleteHook(v reflect.Value, q *Query) (*Query, error) {
	return v.Interface().(BeforeDeleteHook).BeforeDelete(q)
}

func callBeforeDeleteHookSlice(slice reflect.Value, ptr bool, q *Query) (*Query, error) {
	return callHookSlice(slice, ptr, q, callBeforeDeleteHook)
}

//------------------------------------------------------------------------------

type AfterDeleteHook interface {
	AfterDelete(*Query) (*Query, error)
}

var afterDeleteHookType = reflect.TypeOf((*AfterDeleteHook)(nil)).Elem()

func callAfterDeleteHook(v reflect.Value, q *Query) (*Query, error) {
	return v.Interface().(AfterDeleteHook).AfterDelete(q)
}

func callAfterDeleteHookSlice(slice reflect.Value, ptr bool, q *Query) (*Query, error) {
	return callHookSlice(slice, ptr, q, callAfterDeleteHook)
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
