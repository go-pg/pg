# OpenTelemetry instrumentation for go-pg

## Installation

```bash
go get github.com/go-pg/pg/extra/pgotel/v10
```

## Usage

Tracing is enabled by adding a query hook:

```go
import (
	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/extra/pgotel/v10"
)

db := pg.Connect(&pg.Options{...})

db.AddQueryHook(pgotel.NewTracingHook())
```

See [documentation](https://pg.uptrace.dev/tracing/) for more details.
