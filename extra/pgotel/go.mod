module github.com/go-pg/pg/extra/pgotel/v10

go 1.15

replace github.com/go-pg/pg/v10 => ../..

require (
	github.com/go-pg/pg/v10 v10.10.0
	github.com/vmihailenco/msgpack/v5 v5.3.4 // indirect
	go.opentelemetry.io/otel v1.0.0-RC1
	go.opentelemetry.io/otel/trace v1.0.0-RC1
	golang.org/x/crypto v0.0.0-20210616213533-5ff15b29337e // indirect
	golang.org/x/sys v0.0.0-20210630005230-0f9fa26af87c // indirect
)
