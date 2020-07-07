package types

import (
	"reflect"
	"testing"
)

func TestRegisterScanner(t *testing.T) {
	tests := []struct {
		expression  func()
		shouldPanic bool
	}{
		{
			func() {
				RegisterScanner(struct{}{}, nil)
			},
			false,
		},
		{
			func() {
				RegisterScanner(struct{ One string }{}, nil)
				RegisterScanner(struct{ Two string }{}, nil)
			},
			false,
		},
		{
			func() {
				// Scanner will implicitly assign a default scanner
				Scanner(reflect.TypeOf(struct{ Three string }{}))
				// This will override it but will not panic since it is the first
				// explicit call to RegisterScanner for that type
				RegisterScanner(struct{ Three string }{}, nil)
			},
			false,
		},
		{
			func() {
				RegisterScanner(struct{ Four string }{}, nil)
				// This panics because we have already explicitly registered a scanner
				// for this type
				RegisterScanner(struct{ Four string }{}, nil)
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
