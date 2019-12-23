package internal

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)

// Retry backoff with jitter sleep to prevent overloaded conditions during intervals
// https://www.awsarchitectureblog.com/2015/03/backoff.html
func RetryBackoff(retry int, minBackoff, maxBackoff time.Duration) time.Duration {
	if retry < 0 {
		retry = 0
	}

	backoff := minBackoff << uint(retry)
	if backoff > maxBackoff || backoff < minBackoff {
		backoff = maxBackoff
	}

	if backoff == 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(backoff)))
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func GetBuffer() *bytes.Buffer {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func PutBuffer(buf *bytes.Buffer) {
	bufPool.Put(buf)
}
