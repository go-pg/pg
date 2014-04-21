package pg_test

import (
	"io"
)

// a WriteCloser which does nothing in Close()
type NopWriteCloser struct {
	io.Writer
}

func (w *NopWriteCloser) Close() error {
	return nil;
}
