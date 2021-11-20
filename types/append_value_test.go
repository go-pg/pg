package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppend(t *testing.T) {
	t.Run("null pointer", func(t *testing.T) {
		var a *struct{}
		result := Append(nil, a, 1)
		assert.Equal(t, "NULL", string(result))
	})

	t.Run("null map", func(t *testing.T) {
		var a map[string]int
		result := Append(nil, a, 1)
		assert.Equal(t, "NULL", string(result))
	})

	t.Run("null string array", func(t *testing.T) {
		var a []string
		result := Append(nil, a, 1)
		assert.Equal(t, "NULL", string(result))
	})
}
