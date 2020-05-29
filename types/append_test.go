package types

import (
	"database/sql/driver"
	"fmt"
	"testing"
)

type valuer struct {
	v string
}
func (v valuer) Value() (driver.Value, error) {
	return v.v, nil
}

func TestAppend_ShouldAppendValue_WhenStruct(t *testing.T) {
	input := "HelloWorld"
	v := valuer{v: input}
	output := Append([]byte{}, v, 1)

	expectedOutput := fmt.Sprintf("'%s'", input)
	if string(output) != expectedOutput {
		t.Fatalf("Append expect: %s but got: %s", expectedOutput, string(output))
	}
}

func TestAppend_ShouldAppendValue_WhenPtr(t *testing.T) {
	input := "HelloWorld"
	v := &valuer{v: input}
	output := Append([]byte{}, v, 1)

	expectedOutput := fmt.Sprintf("'%s'", input)
	if string(output) != expectedOutput {
		t.Fatalf("Append expect: %s but got: %s", expectedOutput, string(output))
	}
}

func TestAppend_ShouldAppendNull_WhenPtrIsNil(t *testing.T) {
	var v  *valuer = nil
	output := Append([]byte{}, v, 1)

	expectedOutput := "NULL"
	if string(output) != expectedOutput {
		t.Fatalf("Append expect: %s but got: %s", expectedOutput, string(output))
	}
}