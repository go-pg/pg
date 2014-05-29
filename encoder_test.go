package pg_test

import (
	"math"

	. "launchpad.net/gocheck"

	"gopkg.in/pg.v1"
)

type EncoderTest struct{}

var _ = Suite(&EncoderTest{})

func (t *EncoderTest) TestTooManyParamsError(c *C) {
	_, err := pg.FormatQ("", "foo", "bar")
	c.Assert(err.Error(), Equals, "pg: expected 0 parameters, got 2")
}

func (t *EncoderTest) TestTooFewParamsError(c *C) {
	_, err := pg.FormatQ("? ? ?", "foo", "bar")
	c.Assert(err.Error(), Equals, "pg: expected at least 3 parameters, got 2")
}

// TODO: check for overflow?
func (t *EncoderTest) TestUint64(c *C) {
	q, err := pg.FormatQ("?", uint64(math.MaxUint64))
	c.Assert(err, IsNil)
	c.Assert(string(q), Equals, "-1")
}

func (t *EncoderTest) TestTypeAlias(c *C) {
	type mystr string
	q, err := pg.FormatQ("?", mystr("hello world"))
	c.Assert(err, IsNil)
	c.Assert(string(q), Equals, "'hello world'")
}

func (t *EncoderTest) TestPointers(c *C) {
	var x *int
	q, err := pg.FormatQ("?", x)
	c.Assert(err, IsNil)
	c.Assert(string(q), Equals, "NULL")

	y := new(int)
	*y = 42
	q, err = pg.FormatQ("?", y)
	c.Assert(err, IsNil)
	c.Assert(string(q), Equals, "42")

	q, err = pg.FormatQ("?", nil)
	c.Assert(err, IsNil)
	c.Assert(string(q), Equals, "NULL")
}

type structFormatter struct {
	Foo string
}

func (structFormatter) Meth() string {
	return "value"
}

func (structFormatter) MethWithArgs(string) string {
	return "value"
}

func (structFormatter) MethWithCompositeReturn() (string, string) {
	return "value1", "value2"
}

type embeddedStructFormatter struct {
	*structFormatter
}

func (embeddedStructFormatter) Meth2() string {
	return "value2"
}

func (t *EncoderTest) TestStruct(c *C) {
	{
		src := struct{ Foo string }{"bar"}
		q, err := pg.FormatQ("?bar", src)
		c.Assert(err.Error(), Equals, `pg: cannot map "bar"`)
		c.Assert(string(q), Equals, "")
	}

	{
		src := struct{ S1, S2 string }{"value1", "value2"}
		q, err := pg.FormatQ("? ?s1 ? ?s2 ?", "one", "two", "three", src)
		c.Assert(err, IsNil)
		c.Assert(string(q), Equals, "'one' 'value1' 'two' 'value2' 'three'")
	}

	{
		src := &structFormatter{}
		_, err := pg.FormatQ("?MethWithArgs", src)
		c.Assert(err.Error(), Equals, `pg: cannot map "MethWithArgs"`)
	}

	{
		src := &structFormatter{}
		_, err := pg.FormatQ("?MethWithCompositeReturn", src)
		c.Assert(err.Error(), Equals, `pg: cannot map "MethWithCompositeReturn"`)
	}

	{
		src := &structFormatter{"bar"}
		q, err := pg.FormatQ("?foo ?Meth", src)
		c.Assert(err, IsNil)
		c.Assert(string(q), Equals, "'bar' 'value'")
	}

	{
		src := &embeddedStructFormatter{&structFormatter{"bar"}}
		q, err := pg.FormatQ("?foo ?Meth ?Meth2", src)
		c.Assert(err, IsNil)
		c.Assert(string(q), Equals, "'bar' 'value' 'value2'")
	}
}

func (t *EncoderTest) TestFormatInts(c *C) {
	q, err := pg.FormatQ("?", pg.Ints{1, 2, 3})
	c.Assert(err, IsNil)
	c.Assert(string(q), Equals, "1,2,3")
}

func (t *EncoderTest) TestFormatStrings(c *C) {
	q, err := pg.FormatQ("?", pg.Strings{"hello", "world"})
	c.Assert(err, IsNil)
	c.Assert(string(q), Equals, "'hello','world'")
}
