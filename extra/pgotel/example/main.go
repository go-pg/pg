package main

import (
	"context"

	"github.com/go-pg/pg/extra/pgotel/v10"
	"github.com/go-pg/pg/v10"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var tracer = otel.Tracer("app_or_package_name")

func main() {
	exporter, err := stdout.NewExporter(stdout.WithPrettyPrint())
	if err != nil {
		panic(err)
	}

	provider := sdktrace.NewTracerProvider()
	provider.RegisterSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter))

	otel.SetTracerProvider(provider)

	db := pg.Connect(&pg.Options{
		Addr:     ":5432",
		User:     "postgres",
		Database: "example",
	})
	defer db.Close()

	db.AddQueryHook(pgotel.NewTracingHook())

	ctx, span := tracer.Start(context.TODO(), "main")
	defer span.End()

	if err := db.Ping(ctx); err != nil {
		panic(err)
	}
}
