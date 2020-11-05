package pool_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-pg/pg/v10/internal/pool"
)

func benchmarkPoolGetPut(b *testing.B, poolSize int) {
	ctx := context.Background()
	connPool := pool.NewConnPool(&pool.Options{
		Dialer:             dummyDialer,
		PoolSize:           poolSize,
		PoolTimeout:        time.Second,
		IdleTimeout:        time.Hour,
		IdleCheckFrequency: time.Hour,
	})

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cn, err := connPool.Get(ctx)
			if err != nil {
				b.Fatal(err)
			}
			connPool.Put(ctx, cn)
		}
	})
}

func BenchmarkPoolGetPut10Conns(b *testing.B) {
	benchmarkPoolGetPut(b, 10)
}

func BenchmarkPoolGetPut100Conns(b *testing.B) {
	benchmarkPoolGetPut(b, 100)
}

func BenchmarkPoolGetPut1000Conns(b *testing.B) {
	benchmarkPoolGetPut(b, 1000)
}

func benchmarkPoolGetRemove(b *testing.B, poolSize int) {
	ctx := context.Background()
	connPool := pool.NewConnPool(&pool.Options{
		Dialer:             dummyDialer,
		PoolSize:           poolSize,
		PoolTimeout:        time.Second,
		IdleTimeout:        time.Hour,
		IdleCheckFrequency: time.Hour,
	})

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cn, err := connPool.Get(ctx)
			if err != nil {
				b.Fatal(err)
			}
			connPool.Remove(ctx, cn, nil)
		}
	})
}

func BenchmarkPoolGetRemove10Conns(b *testing.B) {
	benchmarkPoolGetRemove(b, 10)
}

func BenchmarkPoolGetRemove100Conns(b *testing.B) {
	benchmarkPoolGetRemove(b, 100)
}

func BenchmarkPoolGetRemove1000Conns(b *testing.B) {
	benchmarkPoolGetRemove(b, 1000)
}

var columns = [][]byte{
	[]byte("id"),
	[]byte("business_phone"),
	[]byte("display_name"),
	[]byte("given_name"),
	[]byte("job_title"),
	[]byte("mail"),
	[]byte("mobile_phone"),
	[]byte("office_location"),
	[]byte("preferred_language"),
	[]byte("surname"),
	[]byte("user_principal_name"),
}

func BenchmarkColumnAlloc(b *testing.B) {
	columnAlloc := pool.NewColumnAlloc()
	for i := 0; i < b.N; i++ {
		for i, column := range columns {
			columnAlloc.New(int16(i), column)
		}
		columnAlloc.Columns()
		columnAlloc.Reset()
	}
}
