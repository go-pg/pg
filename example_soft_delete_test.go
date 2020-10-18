package pg_test

import (
	"fmt"
	"time"

	"github.com/go-pg/pg/v11/types"
)

type CustomTime struct {
	Time time.Time
}

var _ types.ValueScanner = (*CustomTime)(nil)

func (tm *CustomTime) ScanValue(rd types.Reader, n int) error {
	var err error
	tm.Time, err = types.ScanTime(rd, n)
	return err
}

var _ types.ValueAppender = (*CustomTime)(nil)

func (tm *CustomTime) AppendValue(b []byte, flags int) ([]byte, error) {
	return types.AppendTime(b, tm.Time, flags), nil
}

type Video struct {
	Id        int
	Name      string
	DeletedAt CustomTime `pg:",soft_delete"`
}

func ExampleDB_Model_softDeleteCustom() {
	video1 := &Video{
		Id: 1,
	}
	_, err := pgdb.Model(video1).Insert(ctx)
	panicIf(err)

	// Soft delete.
	_, err = pgdb.Model(video1).WherePK().Delete(ctx)
	panicIf(err)

	// Count visible videos.
	count, err := pgdb.Model((*Video)(nil)).Count(ctx)
	panicIf(err)
	fmt.Println("count", count)

	// Count soft deleted videos.
	deletedCount, err := pgdb.Model((*Video)(nil)).Deleted().Count(ctx)
	panicIf(err)
	fmt.Println("deleted count", deletedCount)

	// Actually delete the video.
	_, err = pgdb.Model(video1).WherePK().ForceDelete(ctx)
	panicIf(err)

	// Count soft deleted videos.
	deletedCount, err = pgdb.Model((*Video)(nil)).Deleted().Count(ctx)
	panicIf(err)
	fmt.Println("deleted count", deletedCount)

	// Output: count 0
	// deleted count 1
	// deleted count 0
}
