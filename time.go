package pg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/go-pg/pg/types"
)

var jsonNull = []byte("null")

// NullTime is a time.Time wrapper that marshals zero time as JSON null and
// PostgreSQL NULL.
type NullTime struct {
	time.Time
}

var _ json.Marshaler = (*NullTime)(nil)
var _ json.Unmarshaler = (*NullTime)(nil)
var _ sql.Scanner = (*NullTime)(nil)
var _ types.ValueAppender = (*NullTime)(nil)

func (tm NullTime) MarshalJSON() ([]byte, error) {
	if tm.IsZero() {
		return jsonNull, nil
	}
	return tm.Time.MarshalJSON()
}

func (tm *NullTime) UnmarshalJSON(b []byte) error {
	if bytes.Equal(b, jsonNull) {
		tm.Time = time.Time{}
		return nil
	}
	return tm.Time.UnmarshalJSON(b)
}

func (tm NullTime) AppendValue(b []byte, quote int) []byte {
	if tm.IsZero() {
		return types.AppendNull(b, quote)
	}
	return types.AppendTime(b, tm.Time, quote)
}

func (tm *NullTime) Scan(b interface{}) error {
	if b == nil {
		tm.Time = time.Time{}
		return nil
	}
	newtm, err := types.ParseTime(b.([]byte))
	if err != nil {
		return err
	}
	tm.Time = newtm
	return nil
}
