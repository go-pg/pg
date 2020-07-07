package types

import (
	"reflect"
	"testing"
)

func TestRegisterAppender(t *testing.T) {
	tests := []struct {
		expression  func()
		shouldPanic bool
	}{
		{
			func() {
				RegisterAppender(struct{}{}, nil)
			},
			false,
		},
		{
			func() {
				RegisterAppender(struct{ One string }{}, nil)
				RegisterAppender(struct{ Two string }{}, nil)
			},
			false,
		},
		{
			func() {
				// Appender will implicitly assign a default appender
				Appender(reflect.TypeOf(struct{ Three string }{}))
				// This will override it but will not panic since it is the first
				// explicit call to RegisterAppender for that type
				RegisterAppender(struct{ Three string }{}, nil)
			},
			false,
		},
		{
			func() {
				RegisterAppender(struct{ Four string }{}, nil)
				// This panics because we have already explicitly registered an appender
				// for this type
				RegisterAppender(struct{ Four string }{}, nil)
			},
			true,
		},
	}

	for testi, test := range tests {
		if causedPanic(test.expression) != test.shouldPanic {
			t.Fatalf(
				"test #%d expected shouldPanic to be %t, was %t",
				testi, test.shouldPanic, !test.shouldPanic)
		}
	}
}

func causedPanic(f func()) bool {
	didPanic := false

	func() {
		defer func() { didPanic = recover() != nil }()

		f()
	}()

	return didPanic
}
