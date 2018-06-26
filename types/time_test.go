package types_test

import (
	"testing"
	"time"

	"github.com/go-pg/pg/types"
)

func TestParseTimeString(t *testing.T) {
	ss := []string{
		"2006-01-02",
		"15:04:05.999999999",
		"15:04:05.999999",
		"15:04:05.999",
		"15:04:05",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05.999999999-07:00:00",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999-07",
		time.Now().Format(time.RFC3339),
		time.Now().In(time.FixedZone("", 3600)).Format(time.RFC3339),
		time.Now().UTC().Format(time.RFC3339),
	}
	for _, s := range ss {
		_, err := types.ParseTimeString(s)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkParseTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		types.ParseTimeString("2001-02-03 04:05:06+07")
	}
}
