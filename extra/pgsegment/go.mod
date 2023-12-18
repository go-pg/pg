module github.com/go-pg/pg/extra/pgsegment/v10

go 1.19

replace github.com/go-pg/pg/v10 => ../..

require (
	github.com/go-pg/pg/v10 v10.12.0
	github.com/segmentio/encoding v0.2.21
)

require (
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/segmentio/asm v1.0.1 // indirect
)
