// +build go1.7

package pg

import (
	"errors"
	"testing"
	"time"
)

func TestParseURL(t *testing.T) {
	cases := []struct {
		url         string
		addr        string
		user        string
		password    string
		database    string
		appName     string
		dialTimeout time.Duration
		tls         bool
		err         error
	}{
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			"",
			0,
			true,
			nil,
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres?sslmode=allow",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			"",
			0,
			true,
			nil,
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres?sslmode=prefer",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			"",
			0,
			true,
			nil,
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres?sslmode=require",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			"",
			0,
			true,
			nil,
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres?sslmode=verify-ca",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			"",
			0,
			true,
			nil,
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres?sslmode=verify-full",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			"",
			0,
			true,
			nil,
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres?sslmode=disable",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			"",
			0,
			false,
			nil,
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres?sslmode=disable&application_name=myApp",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			"myApp",
			0,
			false,
			nil,
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres?application_name=myApp",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			"myApp",
			0,
			false,
			nil,
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"",
			"",
			0,
			true,
			errors.New("pg: database name not provided"),
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com/postgres",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			"",
			0,
			true,
			nil,
		},
		{
			"postgres://vasya:pupkin@somewhere.at.amazonaws.com:5432/postgres?abc=123",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"pupkin",
			"postgres",
			"",
			0,
			true,
			errors.New("pg: options other than 'sslmode', 'application_name' and 'connect_timeout' are not supported"),
		},
		{
			"postgres://vasya@somewhere.at.amazonaws.com:5432/postgres",
			"somewhere.at.amazonaws.com:5432",
			"vasya",
			"",
			"postgres",
			"",
			0,
			true,
			nil,
		},
		{
			"postgres://somewhere.at.amazonaws.com:5432/postgres",
			"somewhere.at.amazonaws.com:5432",
			"postgres",
			"",
			"postgres",
			"",
			0,
			true,
			nil,
		},
		{
			"postgresql://somewhere.at.amazonaws.com:5432/postgres",
			"somewhere.at.amazonaws.com:5432",
			"postgres",
			"",
			"postgres",
			"",
			0,
			true,
			nil,
		},
		{
			"http://google.com/test",
			"google.com:5432",
			"postgres",
			"",
			"test",
			"",
			0,
			true,
			errors.New("pg: invalid scheme: http"),
		},
		{
			"postgresql://pg.example.com:5432/db?connect_timeout=10",
			"pg.example.com:5432",
			"postgres",
			"",
			"db",
			"",
			time.Second * 10,
			true,
			nil,
		},
		{
			"postgresql://pg.example.com:5432/db?connect_timeout=10s",
			"pg.example.com:5432",
			"postgres",
			"",
			"db",
			"",
			0,
			true,
			errors.New("pg: cannot parse connect_timeout option as int"),
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
					t.Fatalf("expected error %q, want %q", err, c.err)
				}
				return
			}
			if c.err != nil && err == nil {
				t.Errorf("expected error %q, got nothing", c.err)
			}
			if o.Addr != c.addr {
				t.Errorf("addr: got %q, want %q", o.Addr, c.addr)
			}
			if o.User != c.user {
				t.Errorf("user: got %q, want %q", o.User, c.user)
			}
			if o.Password != c.password {
				t.Errorf("password: got %q, want %q", o.Password, c.password)
			}
			if o.Database != c.database {
				t.Errorf("database: got %q, want %q", o.Database, c.database)
			}
			if o.ApplicationName != c.appName {
				t.Errorf("appName: got %q, want %q", o.ApplicationName, c.appName)
			}

			if c.tls {
				if o.TLSConfig == nil {
					t.Error("got nil TLSConfig, expected a TLSConfig")
				}
			}
		})
	}
}
