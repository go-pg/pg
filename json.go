package pg

import (
	"encoding/json"
	"io"

	"github.com/go-pg/pg/v9/internal"
	json2 "github.com/segmentio/encoding/json"
)

type JsonProvider = internal.JsonProvider
type JsonDecoder = internal.JsonDecoder
type JsonEncoder = internal.JsonEncoder

func init() {
	SetJsonProvider(SegmentioJson{})
}

// Set JsonProvider used for jsonb fields encoding/decoding
func SetJsonProvider(provider JsonProvider) {
	internal.Json = provider
}

var _ JsonProvider = (*StdJson)(nil)

type StdJson struct {
}

func (s StdJson) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (s StdJson) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (s StdJson) NewEncoder(w io.Writer) JsonEncoder {
	return json.NewEncoder(w)
}

func (s StdJson) NewDecoder(r io.Reader) JsonDecoder {
	return json.NewDecoder(r)
}

var _ JsonProvider = (*SegmentioJson)(nil)

type SegmentioJson struct {
}

func (s SegmentioJson) Marshal(v interface{}) ([]byte, error) {
	return json2.Marshal(v)
}

func (s SegmentioJson) Unmarshal(data []byte, v interface{}) error {
	return json2.Unmarshal(data, v)
}

func (s SegmentioJson) NewEncoder(w io.Writer) JsonEncoder {
	return json2.NewEncoder(w)
}

func (s SegmentioJson) NewDecoder(r io.Reader) JsonDecoder {
	return json2.NewDecoder(r)
}
