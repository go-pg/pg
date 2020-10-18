package pgsegment

import (
	"io"

	"github.com/go-pg/pg/v11/pgjson"
	"github.com/segmentio/encoding/json"
)

var _ pgjson.Provider = (*SegmentJSONProvider)(nil)

type SegmentJSONProvider struct{}

func (SegmentJSONProvider) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (SegmentJSONProvider) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (SegmentJSONProvider) NewEncoder(w io.Writer) pgjson.Encoder {
	return json.NewEncoder(w)
}

func (SegmentJSONProvider) NewDecoder(r io.Reader) pgjson.Decoder {
	return json.NewDecoder(r)
}
