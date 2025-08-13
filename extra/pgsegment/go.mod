module github.com/go-pg/pg/extra/pgsegment/v10

go 1.23.0

toolchain go1.23.2

replace github.com/go-pg/pg/v10 => ../..

require (
	github.com/go-pg/pg/v10 v10.15.0
	github.com/segmentio/encoding v0.2.21
)

require (
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/segmentio/asm v1.0.1 // indirect
)
