package orm

import (
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/go-pg/pg/v9/internal"
	"github.com/go-pg/pg/v9/types"
	"github.com/vmihailenco/msgpack/v4"
)

func msgpackAppender(_ reflect.Type) types.AppenderFunc {
	return func(b []byte, v reflect.Value, flags int) []byte {
		buf := internal.GetBuffer()
		defer internal.PutBuffer(buf)

		if err := msgpack.NewEncoder(buf).EncodeValue(v); err != nil {
			return types.AppendError(b, err)
		}

		return types.AppendBytes(b, buf.Bytes(), flags)
	}
}

func msgpackScanner(_ reflect.Type) types.ScannerFunc {
	return func(v reflect.Value, rd types.Reader, n int) error {
		if n == -1 {
			return nil
		}

		c, err := rd.ReadByte()
		if err != nil {
			return err
		}
		if c != '\\' {
			return fmt.Errorf("pg: got %q, wanted '\\'", c)
		}

		c, err = rd.ReadByte()
		if err != nil {
			return err
		}
		if c != 'x' {
			return fmt.Errorf("pg: got %q, wanted 'x'", c)
		}

		if err := msgpack.NewDecoder(hex.NewDecoder(rd)).DecodeValue(v); err != nil {
			return err
		}

		return nil
	}
}
