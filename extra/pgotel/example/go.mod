module example

go 1.16

replace github.com/go-pg/pg/v10 => ../../..

replace github.com/go-pg/pg/extra/pgotel/v10 => ../

require (
	github.com/go-pg/pg/extra/pgotel/v10 v10.10.4
	github.com/go-pg/pg/v10 v10.10.4
	go.opentelemetry.io/otel v1.0.0-RC2
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.0.0-RC2
	go.opentelemetry.io/otel/sdk v1.0.0-RC2
)
