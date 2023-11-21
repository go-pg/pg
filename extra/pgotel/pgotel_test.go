package pgotel

import (
	"testing"

	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
)

// mockResult is a mock implementation of the Result interface
type mockResult struct {
	model        orm.Model
	rowsAffected int
	rowsReturned int
}

func (m mockResult) Model() orm.Model {
	return m.model
}

func (m mockResult) RowsAffected() int {
	return m.rowsAffected
}

func (m mockResult) RowsReturned() int {
	return m.rowsReturned
}

func TestHasResults(t *testing.T) {
	// Define test cases
	testCases := []struct {
		name     string
		event    *pg.QueryEvent
		expected bool
	}{
		{
			name:     "Nil Result",
			event:    &pg.QueryEvent{Result: nil},
			expected: false,
		},
		{
			name:     "Nil Pointer Result",
			event:    &pg.QueryEvent{Result: (*mockResult)(nil)},
			expected: false,
		},
		{
			name:     "Non-Nil Result",
			event:    &pg.QueryEvent{Result: mockResult{rowsAffected: 1, rowsReturned: 1}},
			expected: true,
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := hasResults(tc.event)
			if result != tc.expected {
				t.Errorf("hasResults(%v) = %v, want %v", tc.event, result, tc.expected)
			}
		})
	}
}
