package pgext

import (
	"context"
	"runtime"
	"strings"

	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/trace"
	"google.golang.org/grpc/codes"
)

type sqlOperation interface {
	Operation() string
}

// OpenTelemetryHook is a pg.QueryHook that adds OpenTemetry instrumentation.
type OpenTelemetryHook struct{}

var _ pg.QueryHook = (*OpenTelemetryHook)(nil)

func (h OpenTelemetryHook) BeforeQuery(ctx context.Context, evt *pg.QueryEvent) (context.Context, error) {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx, nil
	}

	ctx, _ = global.Tracer("github.com/go-pg/pg").Start(ctx, "")
	return ctx, nil
}

func (h OpenTelemetryHook) AfterQuery(ctx context.Context, evt *pg.QueryEvent) error {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return nil
	}
	defer span.End()

	var query string
	if v, ok := evt.Query.(sqlOperation); ok && v.Operation() == orm.InsertOp {
		b, err := evt.UnformattedQuery()
		if err != nil {
			return err
		}
		query = string(b)
	} else {
		b, err := evt.FormattedQuery()
		if err != nil {
			return err
		}
		query = string(b)
	}

	const queryLimit = 2000
	if len(query) > queryLimit {
		query = query[:queryLimit]
	}

	fn, file, line := funcFileLine("github.com/go-pg/pg")

	attrs := make([]kv.KeyValue, 0, 10)
	attrs = append(attrs,
		kv.String("db.system", "postgres"),
		kv.String("db.statement", query),

		kv.String("frame.func", fn),
		kv.String("frame.file", file),
		kv.Int("frame.line", line),
	)

	if db, ok := evt.DB.(*pg.DB); ok {
		opt := db.Options()
		attrs = append(attrs,
			kv.String("db.connection_string", opt.Addr),
			kv.String("db.user", opt.User),
			kv.String("db.name", opt.Database),
		)
	}

	if evt.Err != nil {
		span.SetStatus(codes.Internal, "")
		span.RecordError(ctx, evt.Err)
	} else if evt.Result != nil {
		numRow := evt.Result.RowsAffected()
		if numRow == 0 {
			numRow = evt.Result.RowsReturned()
		}
		attrs = append(attrs, kv.Int("db.rows_affected", numRow))
	}

	span.SetAttributes(attrs...)

	return nil
}

func funcFileLine(pkg string) (string, string, int) {
	const depth = 16
	var pcs [depth]uintptr
	n := runtime.Callers(3, pcs[:])
	ff := runtime.CallersFrames(pcs[:n])

	var fn, file string
	var line int
	for {
		f, ok := ff.Next()
		if !ok {
			break
		}
		fn, file, line = f.Function, f.File, f.Line
		if !strings.Contains(fn, pkg) {
			break
		}
	}

	if ind := strings.LastIndexByte(fn, '/'); ind != -1 {
		fn = fn[ind+1:]
	}

	return fn, file, line
}
