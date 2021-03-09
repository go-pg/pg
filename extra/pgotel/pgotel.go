package pgotel

import (
	"context"
	"runtime"
	"strings"

	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("github.com/go-pg/pg")

type queryOperation interface {
	Operation() orm.QueryOp
}

// TracingHook is a pg.QueryHook that adds OpenTelemetry instrumentation.
type TracingHook struct{}

var _ pg.QueryHook = (*TracingHook)(nil)

func (h TracingHook) BeforeQuery(ctx context.Context, _ *pg.QueryEvent) (context.Context, error) {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx, nil
	}

	ctx, _ = tracer.Start(ctx, "")
	return ctx, nil
}

func (h TracingHook) AfterQuery(ctx context.Context, evt *pg.QueryEvent) error {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return nil
	}
	defer span.End()

	var operation orm.QueryOp

	if v, ok := evt.Query.(queryOperation); ok {
		operation = v.Operation()
	}

	var query string
	if operation == orm.InsertOp {
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

	if operation != "" {
		span.SetName(string(operation))
	} else {
		name := query
		if idx := strings.IndexByte(name, ' '); idx > 0 {
			name = name[:idx]
		}
		if len(name) > 20 {
			name = name[:20]
		}
		span.SetName(strings.TrimSpace(name))
	}

	const queryLimit = 5000
	if len(query) > queryLimit {
		query = query[:queryLimit]
	}

	fn, file, line := funcFileLine("github.com/go-pg/pg")

	attrs := make([]attribute.KeyValue, 0, 10)
	attrs = append(attrs,
		attribute.String("db.system", "postgresql"),
		attribute.String("db.statement", query),

		attribute.String("code.function", fn),
		attribute.String("code.filepath", file),
		attribute.Int("code.lineno", line),
	)

	if db, ok := evt.DB.(*pg.DB); ok {
		opt := db.Options()
		attrs = append(attrs,
			attribute.String("db.connection_string", opt.Addr),
			attribute.String("db.user", opt.User),
			attribute.String("db.name", opt.Database),
		)
	}

	if evt.Err != nil {
		switch evt.Err {
		case pg.ErrNoRows, pg.ErrMultiRows:
		default:
			span.RecordError(evt.Err)
			span.SetStatus(codes.Error, evt.Err.Error())
		}
	} else if evt.Result != nil {
		numRow := evt.Result.RowsAffected()
		if numRow == 0 {
			numRow = evt.Result.RowsReturned()
		}
		attrs = append(attrs, attribute.Int("db.rows_affected", numRow))
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
