package netmap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilterIn(t *testing.T) {
	f := FilterIn("abc", "def", "oh no")

	require.True(t, f.Check("abc"))
	require.True(t, f.Check("oh no"))
	require.False(t, f.Check(""))
	require.False(t, f.Check("abcd"))
	require.True(t, f.Check("def"))
}

func TestFilterNotIn(t *testing.T) {
	f := FilterNotIn("abc", "def", "oh no")

	require.True(t, f.Check(""))
	require.False(t, f.Check("abc"))
	require.True(t, f.Check("abcd"))
	require.False(t, f.Check("oh no"))
	require.False(t, f.Check("def"))
}

func TestFilterEQ(t *testing.T) {
	for _, s := range []string{"abcdef", "lul"} {
		f := FilterEQ(s)
		require.True(t, f.Check(s))
		require.False(t, f.Check("not"))
	}
}

func TestFilterNE(t *testing.T) {
	for _, s := range []string{"abcdef", "lul"} {
		f := FilterNE(s)
		require.False(t, f.Check(s))
		require.True(t, f.Check("not"))
	}
}

func TestFilterGT(t *testing.T) {
	var f *SimpleFilter

	f = FilterGT(20)
	require.False(t, f.Check("19"))
	require.False(t, f.Check("20"))
	require.True(t, f.Check("21"))
	require.True(t, f.Check("nan"))

	f = FilterGT(-11)
	require.False(t, f.Check("-12"))
	require.False(t, f.Check("-11"))
	require.True(t, f.Check("0"))
	require.True(t, f.Check("nan"))
}

func TestFilterGE(t *testing.T) {
	var f *SimpleFilter

	f = FilterGE(20)
	require.False(t, f.Check("19"))
	require.True(t, f.Check("20"))
	require.True(t, f.Check("21"))
	require.True(t, f.Check("nan"))

	f = FilterGE(-11)
	require.False(t, f.Check("-12"))
	require.True(t, f.Check("-11"))
	require.True(t, f.Check("0"))
	require.True(t, f.Check("nan"))
}

func TestFilterLT(t *testing.T) {
	var f *SimpleFilter

	f = FilterLT(20)
	require.True(t, f.Check("19"))
	require.False(t, f.Check("20"))
	require.False(t, f.Check("21"))
	require.True(t, f.Check("nan"))

	f = FilterLT(-11)
	require.True(t, f.Check("-12"))
	require.False(t, f.Check("-11"))
	require.False(t, f.Check("0"))
	require.True(t, f.Check("nan"))
}

func TestFilterLE(t *testing.T) {
	var f *SimpleFilter

	f = FilterLE(20)
	require.True(t, f.Check("19"))
	require.True(t, f.Check("20"))
	require.False(t, f.Check("21"))
	require.True(t, f.Check("nan"))

	f = FilterLE(-11)
	require.True(t, f.Check("-12"))
	require.True(t, f.Check("-11"))
	require.False(t, f.Check("0"))
	require.True(t, f.Check("nan"))
}
