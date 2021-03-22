module github.com/go-pg/pg/extra/pgotel

go 1.15

replace github.com/go-pg/pg/v10 => ../..

require (
	github.com/go-pg/pg/v10 v10.5.0
	go.opentelemetry.io/otel v0.18.0
	go.opentelemetry.io/otel/trace v0.18.0
)
