module github.com/go-pg/pg/extra/pgsegment/v10

go 1.15

replace github.com/go-pg/pg/v10 => ../..

require (
	github.com/go-pg/pg/v10 v10.11.2
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/segmentio/encoding v0.2.21
)
