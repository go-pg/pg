// +build go1.7

package pg

import (
	"errors"
	"testing"
)

func TestParseURL(t *testing.T) {
	cases := []struct {
		url      string
		addr     string
		user     string
		password string
		database string
		tls      bool
		err      error
	}{
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			true,
			nil,
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres?sslmode=allow",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			true,
			nil,
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres?sslmode=prefer",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			true,
			nil,
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres?sslmode=require",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			true,
			errors.New("pg: sslmode 'require' is not supported"),
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres?sslmode=verify-ca",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			true,
			errors.New("pg: sslmode 'verify-ca' is not supported"),
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres?sslmode=verify-full",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			true,
			errors.New("pg: sslmode 'verify-full' is not supported"),
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres?sslmode=disable",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			false,
			nil,
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"",
			true,
			errors.New("pg: database name not provided"),
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com/postgres",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			true,
			nil,
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres?abc=123",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			true,
			errors.New("pg: options other than 'sslmode' are not supported"),
		},
		{
			"postgres://vasya@somewhere.at.amazonaws.com:5432/postgres",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"",
			"postgres",
			true,
			nil,
		},
		{
			"postgres://somewhere.at.amazonaws.com:5432/postgres",
			"somewhere.at.amazonaws.com:5432",
			"",
			"",
			"postgres",
			true,
			nil,
		},
		{
			"http://google.com/test",
			"google.com:5432",
			"",
			"",
			"test",
			true,
			errors.New("pg: invalid scheme: http"),
		},
	}

	for _, c := range cases {
		t.Run(c.url, func(t *testing.T) {
			o, err := ParseURL(c.url)
			if c.err == nil && err != nil {
				t.Fatalf("unexpected error: '%q'", err)
				return
			}
			if c.err != nil && err != nil {
				if c.err.Error() != err.Error() {
					t.Fatalf("got %q, want %q", err, c.err)
				}
				return
			}
			if c.err != nil && err == nil {
				t.Errorf("expected error %q, got nothing", c.err)
			}
			if o.Addr != c.addr {
				t.Errorf("got %q, want %q", o.Addr, c.addr)
			}
			if o.User != c.user {
				t.Errorf("got %q, want %q", o.User, c.user)
			}
			if o.Password != c.password {
				t.Errorf("got %q, want %q", o.Password, c.password)
			}
			if o.Database != c.database {
				t.Errorf("got %q, want %q", o.Database, c.database)
			}
			if o.TLSConfig == nil && c.tls {
				t.Error("got nil TLSConfig, expected a TLSConfig")
			}
		})
	}
}
