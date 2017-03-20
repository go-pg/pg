package internal

import "time"

const retryBackoff = 250 * time.Millisecond
const maxRetryBackoff = 5 * time.Second

func RetryBackoff(retry int) time.Duration {
	if retry < 0 {
		retry = 0
	}

	backoff := retryBackoff << uint(retry)
	if backoff > maxRetryBackoff {
		return maxRetryBackoff
	}
	return backoff
}
