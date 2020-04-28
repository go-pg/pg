package orm

import (
	"encoding/hex"
	"reflect"

	"github.com/vmihailenco/bufpool"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/go-pg/pg/v10/types"
)

var msgpackPool bufpool.Pool

func msgpackAppender(_ reflect.Type) types.AppenderFunc {
	return func(b []byte, v reflect.Value, flags int) []byte {
		buf := msgpackPool.Get()
		defer msgpackPool.Put(buf)

		enc := msgpack.GetEncoder()
		defer msgpack.PutEncoder(enc)

		enc.Reset(buf)
		if err := enc.EncodeValue(v); err != nil {
			return types.AppendError(b, err)
		}

		return types.AppendBytes(b, buf.Bytes(), flags)
	}
}

func msgpackScanner(_ reflect.Type) types.ScannerFunc {
	return func(v reflect.Value, rd types.Reader, n int) error {
		if n <= 0 {
			return nil
		}

		decLen := hex.DecodedLen(n - 2)

		buf := bufpool.Get(decLen)
		defer bufpool.Put(buf)

		if err := types.ReadBytes(rd, buf.Bytes()); err != nil {
			return err
		}

		dec := msgpack.GetDecoder()
		defer msgpack.PutDecoder(dec)

		dec.Reset(buf)
		if err := dec.DecodeValue(v); err != nil {
			return err
		}

		return nil
	}
}
