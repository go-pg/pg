package pgjson

import (
	"encoding/json"
	"io"

	json2 "github.com/segmentio/encoding/json"
)

var _ Provider = (*StdProvider)(nil)

type StdProvider struct{}

func (StdProvider) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (StdProvider) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (StdProvider) NewEncoder(w io.Writer) Encoder {
	return json.NewEncoder(w)
}

func (StdProvider) NewDecoder(r io.Reader) Decoder {
	return json.NewDecoder(r)
}

var _ Provider = (*SegmentioProvider)(nil)

type SegmentioProvider struct{}

func (SegmentioProvider) Marshal(v interface{}) ([]byte, error) {
	return json2.Marshal(v)
}

func (SegmentioProvider) Unmarshal(data []byte, v interface{}) error {
	return json2.Unmarshal(data, v)
}

func (SegmentioProvider) NewEncoder(w io.Writer) Encoder {
	return json2.NewEncoder(w)
}

func (SegmentioProvider) NewDecoder(r io.Reader) Decoder {
	return json2.NewDecoder(r)
}
