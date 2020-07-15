package pg_test

import (
	"fmt"
	"time"

	"github.com/go-pg/pg/v10/orm"
	"github.com/go-pg/pg/v10/types"
)

const pgTimeFormat = "15:04:05.999999999"

type Time struct {
	time.Time
}

var _ types.ValueAppender = (*Time)(nil)

func (tm Time) AppendValue(b []byte, flags int) ([]byte, error) {
	if flags == 1 {
		b = append(b, '\'')
	}
	b = tm.UTC().AppendFormat(b, pgTimeFormat)
	if flags == 1 {
		b = append(b, '\'')
	}
	return b, nil
}

var _ types.ValueScanner = (*Time)(nil)

func (tm *Time) ScanValue(rd types.Reader, n int) error {
	if n <= 0 {
		tm.Time = time.Time{}
		return nil
	}

	tmp, err := rd.ReadFullTemp()
	if err != nil {
		return err
	}

	tm.Time, err = time.ParseInLocation(pgTimeFormat, string(tmp), time.UTC)
	if err != nil {
		return err
	}

	return nil
}

type Event struct {
	Id   int
	Time Time `pg:"type:time"`
}

func ExampleDB_Model_customType() {
	db := connect()
	defer db.Close()

	err := db.Model((*Event)(nil)).CreateTable(&orm.CreateTableOptions{
		Temp: true,
	})
	panicIf(err)

	_, err = db.Model(&Event{
		Time: Time{time.Date(0, 0, 0, 12, 00, 00, 00, time.UTC)}, // noon
	}).Insert()
	panicIf(err)

	evt := new(Event)
	err = db.Model(evt).Select()
	panicIf(err)

	fmt.Println(evt.Time)
	// Output: 0000-01-01 12:00:00 +0000 UTC
}
