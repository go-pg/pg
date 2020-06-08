package pgext

import (
	"context"
	"log"

	"github.com/go-pg/pg/v10"
)

// DebugHook is a query hook that logs the query and the error if there are any.
// It can be installed with:
//
//   db.AddQueryHook(pgext.DebugHook{})
type DebugHook struct{}

var _ pg.QueryHook = (*DebugHook)(nil)

func (DebugHook) BeforeQuery(ctx context.Context, evt *pg.QueryEvent) (context.Context, error) {
	q, err := evt.FormattedQuery()
	if err != nil {
		return nil, err
	}

	if evt.Err != nil {
		log.Printf("Error %s executing query:\n%s\n", evt.Err, q)
	} else {
		log.Printf("%s", q)
	}

	return ctx, nil
}

func (DebugHook) AfterQuery(context.Context, *pg.QueryEvent) error {
	return nil
}
