package netgraph

import (
	"testing"

	. "github.com/onsi/gomega"
)

func TestFilterIn(t *testing.T) {
	g := NewGomegaWithT(t)

	f := FilterIn("abc", "def", "oh no")

	g.Expect(f.Check("abc")).To(BeTrue())
	g.Expect(f.Check("oh no")).To(BeTrue())
	g.Expect(f.Check("")).To(BeFalse())
	g.Expect(f.Check("abcd")).To(BeFalse())
	g.Expect(f.Check("def")).To(BeTrue())
}

func TestFilterNotIn(t *testing.T) {
	g := NewGomegaWithT(t)

	f := FilterNotIn("abc", "def", "oh no")

	g.Expect(f.Check("")).To(BeTrue())
	g.Expect(f.Check("abc")).To(BeFalse())
	g.Expect(f.Check("abcd")).To(BeTrue())
	g.Expect(f.Check("oh no")).To(BeFalse())
	g.Expect(f.Check("def")).To(BeFalse())
}

func TestFilterEQ(t *testing.T) {
	g := NewGomegaWithT(t)

	for _, s := range []string{"abcdef", "lul"} {
		f := FilterEQ(s)
		g.Expect(f.Check(s)).To(BeTrue())
		g.Expect(f.Check("not")).To(BeFalse())
	}
}

func TestFilterNE(t *testing.T) {
	g := NewGomegaWithT(t)

	for _, s := range []string{"abcdef", "lul"} {
		f := FilterNE(s)
		g.Expect(f.Check(s)).To(BeFalse())
		g.Expect(f.Check("not")).To(BeTrue())
	}
}

func TestFilterGT(t *testing.T) {
	var f *SimpleFilter

	g := NewGomegaWithT(t)

	f = FilterGT(20)
	g.Expect(f.Check("19")).To(BeFalse())
	g.Expect(f.Check("20")).To(BeFalse())
	g.Expect(f.Check("21")).To(BeTrue())
	g.Expect(f.Check("nan")).To(BeTrue())

	f = FilterGT(-11)
	g.Expect(f.Check("-12")).To(BeFalse())
	g.Expect(f.Check("-11")).To(BeFalse())
	g.Expect(f.Check("0")).To(BeTrue())
	g.Expect(f.Check("nan")).To(BeTrue())
}

func TestFilterGE(t *testing.T) {
	var f *SimpleFilter

	g := NewGomegaWithT(t)

	f = FilterGE(20)
	g.Expect(f.Check("19")).To(BeFalse())
	g.Expect(f.Check("20")).To(BeTrue())
	g.Expect(f.Check("21")).To(BeTrue())
	g.Expect(f.Check("nan")).To(BeTrue())

	f = FilterGE(-11)
	g.Expect(f.Check("-12")).To(BeFalse())
	g.Expect(f.Check("-11")).To(BeTrue())
	g.Expect(f.Check("0")).To(BeTrue())
	g.Expect(f.Check("nan")).To(BeTrue())
}

func TestFilterLT(t *testing.T) {
	var f *SimpleFilter

	g := NewGomegaWithT(t)

	f = FilterLT(20)
	g.Expect(f.Check("19")).To(BeTrue())
	g.Expect(f.Check("20")).To(BeFalse())
	g.Expect(f.Check("21")).To(BeFalse())
	g.Expect(f.Check("nan")).To(BeTrue())

	f = FilterLT(-11)
	g.Expect(f.Check("-12")).To(BeTrue())
	g.Expect(f.Check("-11")).To(BeFalse())
	g.Expect(f.Check("0")).To(BeFalse())
	g.Expect(f.Check("nan")).To(BeTrue())
}

func TestFilterLE(t *testing.T) {
	var f *SimpleFilter

	g := NewGomegaWithT(t)

	f = FilterLE(20)
	g.Expect(f.Check("19")).To(BeTrue())
	g.Expect(f.Check("20")).To(BeTrue())
	g.Expect(f.Check("21")).To(BeFalse())
	g.Expect(f.Check("nan")).To(BeTrue())

	f = FilterLE(-11)
	g.Expect(f.Check("-12")).To(BeTrue())
	g.Expect(f.Check("-11")).To(BeTrue())
	g.Expect(f.Check("0")).To(BeFalse())
	g.Expect(f.Check("nan")).To(BeTrue())
}
