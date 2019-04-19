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
	ctx context.Context,
	slice reflect.Value,
	ptr bool,
	db DB,
	hook func(context.Context, reflect.Value, DB) error,
) error {
	var firstErr error
	for i := 0; i < slice.Len(); i++ {
		v := slice.Index(i)
		if !ptr {
			v = v.Addr()
		}

		err := hook(ctx, v, db)
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

func callAfterQueryHook(ctx context.Context, v reflect.Value, db DB) error {
	return v.Interface().(afterQueryHook).AfterQuery(ctx, db)
}

func callAfterQueryHookSlice(ctx context.Context, slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(ctx, slice, ptr, db, callAfterQueryHook)
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

func callBeforeSelectQueryHook(ctx context.Context, v reflect.Value, db DB, q *Query) (*Query, error) {
	return v.Interface().(beforeSelectQueryHook).BeforeSelectQuery(ctx, db, q)
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

func callAfterSelectHook(ctx context.Context, v reflect.Value, db DB) error {
	return v.Interface().(afterSelectHook).AfterSelect(ctx, db)
}

func callAfterSelectHookSlice(ctx context.Context, slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(ctx, slice, ptr, db, callAfterSelectHook)
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

func callBeforeInsertHook(ctx context.Context, v reflect.Value, db DB) error {
	return v.Interface().(beforeInsertHook).BeforeInsert(ctx, db)
}

func callBeforeInsertHookSlice(ctx context.Context, slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(ctx, slice, ptr, db, callBeforeInsertHook)
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

func callAfterInsertHook(ctx context.Context, v reflect.Value, db DB) error {
	return v.Interface().(afterInsertHook).AfterInsert(ctx, db)
}

func callAfterInsertHookSlice(ctx context.Context, slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(ctx, slice, ptr, db, callAfterInsertHook)
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

func callBeforeUpdateHook(ctx context.Context, v reflect.Value, db DB) error {
	return v.Interface().(beforeUpdateHook).BeforeUpdate(ctx, db)
}

func callBeforeUpdateHookSlice(ctx context.Context, slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(ctx, slice, ptr, db, callBeforeUpdateHook)
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

func callAfterUpdateHook(ctx context.Context, v reflect.Value, db DB) error {
	return v.Interface().(afterUpdateHook).AfterUpdate(ctx, db)
}

func callAfterUpdateHookSlice(ctx context.Context, slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(ctx, slice, ptr, db, callAfterUpdateHook)
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

func callBeforeDeleteHook(ctx context.Context, v reflect.Value, db DB) error {
	return v.Interface().(beforeDeleteHook).BeforeDelete(ctx, db)
}

func callBeforeDeleteHookSlice(ctx context.Context, slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(ctx, slice, ptr, db, callBeforeDeleteHook)
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

func callAfterDeleteHook(ctx context.Context, v reflect.Value, db DB) error {
	return v.Interface().(afterDeleteHook).AfterDelete(ctx, db)
}

func callAfterDeleteHookSlice(ctx context.Context, slice reflect.Value, ptr bool, db DB) error {
	return callHookSlice(ctx, slice, ptr, db, callAfterDeleteHook)
}
