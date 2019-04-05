package netmap

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

type bucket struct {
	name  string
	nodes []uint32
}

var defaultPivot = []byte("This is default random data")

func newRoot(bs ...bucket) (b Bucket, err error) {
	for i := range bs {
		if err = b.AddBucket(bs[i].name, bs[i].nodes); err != nil {
			return
		}
	}
	return
}

func TestBucket_IsValid(t *testing.T) {
	var (
		b       Bucket
		buckets []bucket
		err     error
	)

	g := NewGomegaWithT(t)

	buckets = []bucket{
		{"/Location:Europe/Country:Germany", []uint32{1, 3}},
		{"/Location:Asia/Country:China", []uint32{2}},
	}
	b, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(b.IsValid()).To(BeTrue(), "simple bucket is valid")

	buckets = []bucket{
		{"/Location:Europe/Country:Germany", []uint32{1, 3}},
		{"/Location:Asia/Country:China", []uint32{1, 2}},
	}
	b, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(b.IsValid()).To(BeFalse(), "different children must not intersect")

	b = Bucket{
		Key:   "Location",
		Value: "Europe",
		nodes: []uint32{1, 2},
		children: []Bucket{
			{
				Key:   "Country",
				Value: "Germany",
				nodes: []uint32{1, 2, 3},
			},
		},
	}
	g.Expect(b.IsValid()).To(BeFalse(), "parent must contain all child nodes")

	// parent can contain more elements
	b = Bucket{
		Key:   "Location",
		Value: "Europe",
		nodes: []uint32{1, 2, 3},
		children: []Bucket{
			{
				Key:   "Country",
				Value: "Germany",
				nodes: []uint32{2},
			},
		},
	}
	g.Expect(b.IsValid()).To(BeTrue(), "parent can contain more nodes")
}

func TestBucket_checkConflicts(t *testing.T) {
	var (
		b1, b2  Bucket
		err     error
		buckets []bucket
	)

	g := NewGomegaWithT(t)

	buckets = []bucket{
		{"/Location:Europe/Country:Germany", []uint32{1, 3}},
		{"/Location:Asia/Country:China", []uint32{1}},
	}
	b1, err = newRoot(buckets[:1]...)
	g.Expect(err).NotTo(HaveOccurred())

	b2, err = newRoot(buckets[1:]...)
	g.Expect(err).NotTo(HaveOccurred())

	g.Expect(b1.CheckConflicts(b2)).To(BeTrue())
	g.Expect(b2.CheckConflicts(b1)).To(BeTrue())

	buckets = []bucket{
		{"/Location:Europe/Country:Germany", []uint32{1, 3}},
		{"/Location:Europe/Country:Germany", []uint32{2, 3}},
	}
	b1, err = newRoot(buckets[:1]...)
	g.Expect(err).NotTo(HaveOccurred())

	b2, err = newRoot(buckets[1:]...)
	g.Expect(err).NotTo(HaveOccurred())

	g.Expect(b1.CheckConflicts(b2)).To(BeFalse())
	g.Expect(b2.CheckConflicts(b1)).To(BeFalse())
}

func TestBucket_Merge(t *testing.T) {
	var (
		b1, b2, exp Bucket
		err         error
		buckets     []bucket
	)

	g := NewGomegaWithT(t)

	buckets = []bucket{
		{"/Location:Europe/Country:Germany", []uint32{1, 3}},
		{"/Location:Asia/Country:China", []uint32{2}},
	}
	b1, err = newRoot(buckets[:1]...)
	g.Expect(err).NotTo(HaveOccurred())

	b2, err = newRoot(buckets[1:]...)
	g.Expect(err).NotTo(HaveOccurred())

	exp, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())

	b1.Merge(b2)
	g.Expect(b1).To(Equal(exp))

	buckets = []bucket{
		{"/Location:Europe/Country:Germany", []uint32{1, 3}},
		{"/Location:Asia/Country:Korea", []uint32{5}},

		{"/Location:Asia/Country:China", []uint32{2, 6}},
		{"/Location:Europe/Country:Germany", []uint32{3, 4}},
	}
	b1, err = newRoot(buckets[:2]...)
	g.Expect(err).NotTo(HaveOccurred())

	b2, err = newRoot(buckets[2:]...)
	g.Expect(err).NotTo(HaveOccurred())

	exp, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())

	b1.Merge(b2)
	g.Expect(b1).To(Equal(exp))
}

func TestBucket_GetSelection(t *testing.T) {
	var (
		err       error
		exp, root Bucket
		r         *Bucket
		buckets   []bucket
		ss        []Select
	)

	g := NewGomegaWithT(t)
	buckets = []bucket{
		{"/Location:Asia/Country:Korea", []uint32{1, 3}},
		{"/Location:Asia/Country:China", []uint32{2}},
		{"/Location:Europe/Country:Germany/City:Hamburg", []uint32{25}},
		{"/Location:Europe/Country:Germany/City:Bremen", []uint32{27, 29}},
		{"/Location:Europe/Country:Spain/City:Madrid", []uint32{17, 18}},
		{"/Location:Europe/Country:Spain/City:Barcelona", []uint32{26, 30}},
		{"/Location:NorthAmerica/Country:USA/City:NewYork", []uint32{19, 20}},
	}

	root, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())

	buckets = []bucket{
		{"/Location:Europe/Country:Germany/City:Hamburg", []uint32{25}},
		{"/Location:Europe/Country:Spain/City:Madrid", []uint32{17, 18}},
	}

	exp, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())

	ss = []Select{
		{Key: "Location", Count: 1},
		{Key: "City", Count: 2},
	}
	r = root.GetSelection(ss, defaultPivot)
	g.Expect(r).NotTo(BeNil())
	g.Expect(r.nodes).To(Equal(exp.nodes))

	buckets = []bucket{
		{"/Location:Europe/Country:Spain/City:Madrid", []uint32{17, 18}},
		{"/Location:NorthAmerica/Country:USA/City:NewYork", []uint32{19, 20}},
	}
	exp, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())

	ss = []Select{
		{Key: "Location", Count: 2},
		{Key: "City", Count: 1},
	}
	r = root.GetSelection(ss, defaultPivot)
	g.Expect(r).NotTo(BeNil())
	g.Expect(r.nodes).To(Equal(exp.nodes))
}

func TestBucket_GetMaxSelection(t *testing.T) {
	var (
		err       error
		exp, root Bucket
		r         *Bucket
		buckets   []bucket
		ss        []Select
		fs        []Filter
	)

	g := NewGomegaWithT(t)

	buckets = []bucket{
		{"/Location:Asia/Country:Korea", []uint32{1, 3}},
		{"/Location:Asia/Country:China", []uint32{2}},
		{"/Location:Asia/Country:Taiwan", []uint32{4, 5}},
		{"/Location:Europe/Country:France", []uint32{6, 7, 8}},
		{"/Location:Europe/Country:Germany/City:Berlin", []uint32{9, 10}},
		{"/Location:Europe/Country:Germany/City:Hamburg", []uint32{25}},
		{"/Location:Europe/Country:Germany/City:Bremen", []uint32{27, 29}},
		{"/Location:Europe/Country:Italy/City:Rome", []uint32{11, 12}},
		{"/Location:Europe/Country:Russia", []uint32{13, 14}},
		{"/Location:Europe/Country:Switzerland", []uint32{15, 16}},
		{"/Location:Europe/Country:Spain/City:Madrid", []uint32{17, 18}},
		{"/Location:Europe/Country:Spain/City:Barcelona", []uint32{26, 30}},
		{"/Location:NorthAmerica/Country:USA", []uint32{19, 20}},
		{"/Location:NorthAmerica/Country:Canada", []uint32{21, 22}},
		{"/Location:NorthAmerica/Country:Mexico", []uint32{23, 24}},
	}

	root, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())

	buckets = []bucket{
		{"/Location:Europe/Country:Germany/City:Berlin", []uint32{9, 10}},
		{"/Location:Europe/Country:Germany/City:Hamburg", []uint32{25}},
		{"/Location:Europe/Country:Germany/City:Bremen", []uint32{27, 29}},
		{"/Location:Europe/Country:Spain/City:Madrid", []uint32{17, 18}},
		{"/Location:Europe/Country:Spain/City:Barcelona", []uint32{26, 30}},
	}
	exp, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())

	// check if subgraph with simple select without node-filters works
	ss = []Select{{Key: "Country", Count: 1}}
	fs = []Filter{{Key: "Country", F: FilterIn("Germany", "Spain")}}
	r = root.GetMaxSelection(SFGroup{Selectors: ss, Filters: fs})
	g.Expect(r).To(Equal(&exp))

	// check if select with count works
	ss = []Select{
		{Key: "Country", Count: 1},
		{Key: "City", Count: 2},
	}
	r = root.GetMaxSelection(SFGroup{Selectors: ss})
	g.Expect(r).To(Equal(&exp))

	// check if count on nodes also works
	ss = []Select{
		{Key: "Location", Count: 1},
		{Key: "Country", Count: 1},
		{Key: NodesBucket, Count: 4},
	}
	fs = []Filter{{Key: "Location", F: FilterEQ("Europe")}}
	r = root.GetMaxSelection(SFGroup{Selectors: ss, Filters: fs})
	g.Expect(r).To(Equal(&exp))

	buckets = []bucket{
		{"/Location:Europe/Country:Spain/City:Madrid", []uint32{17, 18}},
		{"/Location:Europe/Country:Spain/City:Barcelona", []uint32{26, 30}},
	}
	exp, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())

	r = root.GetMaxSelection(SFGroup{
		Selectors: ss,
		Filters:   fs,
		Exclude:   []uint32{9, 27, 29},
	})
	g.Expect(r).To(Equal(&exp))

	r = root.GetMaxSelection(SFGroup{
		Selectors: ss,
		Filters:   fs,
		Exclude:   []uint32{9, 27, 29, 26},
	})
	g.Expect(r).To(BeNil())

	buckets = []bucket{
		{"/Location:Europe/Country:Germany/City:Berlin", []uint32{9, 10}},
		{"/Location:Europe/Country:Germany/City:Hamburg", []uint32{25}},
		{"/Location:Europe/Country:Germany/City:Bremen", []uint32{27, 29}},
		{"/Location:Europe/Country:Italy/City:Rome", []uint32{11, 12}},
		{"/Location:Europe/Country:Spain/City:Madrid", []uint32{17, 18}},
		{"/Location:Europe/Country:Spain/City:Barcelona", []uint32{26, 30}},
	}
	exp, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())

	// check if select with count works
	ss = []Select{
		{Key: "City", Count: 2},
	}
	r = root.GetMaxSelection(SFGroup{Selectors: ss})
	g.Expect(r).To(Equal(&exp))
}

func TestNetMap_GetNodesByOption(t *testing.T) {
	var (
		fr, ge, eu, root Bucket
	)

	g := NewGomegaWithT(t)

	fr = Bucket{
		Key:   "Country",
		Value: "France",
		nodes: []uint32{0, 1, 3},
	}
	ge = Bucket{
		Key:   "Country",
		Value: "Germany",
		nodes: []uint32{2, 4},
	}
	eu = Bucket{
		Key:      "Location",
		Value:    "Europe",
		nodes:    []uint32{0, 1, 2, 3, 4},
		children: []Bucket{fr, ge},
	}
	root = Bucket{
		nodes:    []uint32{0, 1, 2, 3, 4, 5, 6},
		children: []Bucket{eu},
	}

	n1 := root.GetNodesByOption("/Location:Europe/Country:Germany")
	g.Expect(n1).To(Equal([]uint32{2, 4}))

	n2 := root.GetNodesByOption("/Location:Europe/Country:Russia")
	g.Expect(n2).To(HaveLen(0))
}

func TestBucket_AddBucket(t *testing.T) {
	var (
		root, nroot Bucket
		err         error
	)

	g := NewGomegaWithT(t)

	root = Bucket{
		children: []Bucket{{
			Key:   "Location",
			Value: "Europe",
			children: []Bucket{
				{Key: "Country", Value: "France"},
				{Key: "Country", Value: "Germany"},
			},
		}},
	}

	nroot, err = newRoot(
		bucket{"/Location:Europe", nil},
		bucket{"/Location:Europe/Country:France", nil},
		bucket{"/Location:Europe/Country:Germany", nil},
	)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(nroot).To(Equal(root))

	// we must correctly handle addition of options without existing parent
	nroot, err = newRoot(
		bucket{"/Location:Europe/Country:France", nil},
		bucket{"/Location:Europe/Country:Germany", nil},
	)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(nroot).To(Equal(root))

	// nothing should happen if we add an already existing option
	err = nroot.AddBucket("/Location:Europe", nil)
	g.Expect(err).NotTo(HaveOccurred())

	g.Expect(nroot).To(Equal(root))
}

func TestBucket_AddNode(t *testing.T) {
	var (
		nroot Bucket
		ns    []uint32
		err   error
	)

	g := NewGomegaWithT(t)

	nroot, err = newRoot(
		bucket{"/Location:Europe/Country:France", []uint32{1, 3}},
		bucket{"/Location:Europe/Country:Germany", []uint32{7}},
	)
	g.Expect(err).NotTo(HaveOccurred())

	ns = nroot.GetNodesByOption("/Location:Europe/Country:Germany")
	g.Expect(ns).To(Equal([]uint32{7}))

	ns = nroot.GetNodesByOption("/Location:Europe")
	g.Expect(ns).To(Equal([]uint32{1, 3, 7}))
}

func TestNetMap_AddNode(t *testing.T) {
	var (
		root Bucket
		err  error
	)

	g := NewGomegaWithT(t)

	root, err = newRoot(
		bucket{"/Location:Europe/Country:France", nil},
		bucket{"/Location:Europe/Country:Germany", nil},
	)
	g.Expect(err).NotTo(HaveOccurred())

	err = root.AddNode(1, "/Location:Europe/Country:France")
	g.Expect(err).NotTo(HaveOccurred())
	err = root.AddNode(2, "/Location:Europe/Country:France")
	g.Expect(err).NotTo(HaveOccurred())
	err = root.AddNode(3, "/Location:Europe/Country:Germany")
	g.Expect(err).NotTo(HaveOccurred())

	ns := root.GetNodesByOption("/Location:Europe/Country:Germany")
	g.Expect(ns).To(Equal([]uint32{3}))

	ns = root.GetNodesByOption("/Location:Europe")
	g.Expect(ns).To(Equal([]uint32{1, 2, 3}))
}

func TestBucket_MarshalBinary(t *testing.T) {
	var (
		before, after Bucket
		data          []byte
		err           error
	)

	g := NewGomegaWithT(t)

	before, err = newRoot(
		bucket{"/Location:Europe", []uint32{1}},
		bucket{"/Location:Asia", []uint32{2}},
	)
	g.Expect(err).NotTo(HaveOccurred())

	data, err = before.MarshalBinary()
	g.Expect(err).NotTo(HaveOccurred())
	err = after.UnmarshalBinary(data)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(before).To(Equal(after))
}

func TestBucket_Nodelist(t *testing.T) {
	var (
		nodes   []uint32
		root    Bucket
		buckets []bucket
		err     error
	)
	g := NewGomegaWithT(t)

	buckets = []bucket{
		{"/Location:Asia/Country:Korea", []uint32{1, 3}},
		{"/Location:Asia/Country:China", []uint32{2}},
		{"/Location:Asia/Country:Taiwan", []uint32{4, 5}},
		{"/Location:Europe/Country:France", []uint32{6, 7, 8}},
		{"/Location:Europe/Country:Germany/City:Berlin", []uint32{9, 10}},
		{"/Location:Europe/Country:Italy/City:Rome", []uint32{11, 12}},
		{"/Location:Europe/Country:Russia", []uint32{13, 14}},
		{"/Location:Europe/Country:Switzerland", []uint32{15, 16}},
		{"/Location:Europe/Country:Spain/City:Madrid", []uint32{17, 18}},
		{"/Location:NorthAmerica/Country:USA", []uint32{19, 20}},
		{"/Location:NorthAmerica/Country:Canada", []uint32{21, 22}},
		{"/Location:NorthAmerica/Country:Mexico", []uint32{23, 24}},
	}

	root, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())

	root.nodes = nil

	root.fillNodes()
	nodes = root.Nodelist()
	g.Expect(nodes).To(HaveLen(24))
	for i := uint32(1); i <= 24; i++ {
		g.Expect(nodes).To(ContainElement(i))
	}
}

func TestNetMap_FindGraph(t *testing.T) {
	var (
		ns         []uint32
		nodesByLoc map[string][]uint32
		root, exp  Bucket
		c          *Bucket
		ss         []Select
		fs         []Filter
		err        error
	)

	g := NewGomegaWithT(t)

	buckets := []bucket{
		{"/Location:Asia/Country:Korea", []uint32{1, 3}},
		{"/Location:Asia/Country:China", []uint32{2}},
		{"/Location:Europe/Country:France/City:Paris", []uint32{6, 7, 8}},
		{"/Location:Europe/Country:Germany", []uint32{9, 10}},
		{"/Location:Europe/Country:Italy", []uint32{11, 12}},
		{"/Location:Europe/Country:Russia/City:Moscow", []uint32{13, 14}},
		{"/Location:Europe/Country:Switzerland", []uint32{15, 16}},
		{"/Location:Europe/Country:Spain/City:Madrid", []uint32{17, 18}},
		{"/Location:NorthAmerica/Country:USA/City:NewYork", []uint32{19, 20}},
		{"/Location:NorthAmerica/Country:Canada", []uint32{21, 22}},
		{"/Location:NorthAmerica/Country:Mexico", []uint32{23, 24}},
		{"/Type:SSD", []uint32{6, 7, 8, 13}},
		{"/Type:HDD", []uint32{14, 21, 22}},
	}
	root, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())

	ss = []Select{{Key: NodesBucket, Count: 6}}
	c = root.FindGraph(nil, SFGroup{Selectors: ss})
	g.Expect(c).NotTo(BeNil())
	g.Expect(c.Nodelist()).To(HaveLen(6))
	for _, r := range c.Nodelist() {
		g.Expect([]uint32{1, 2, 3, 6, 7, 8}).To(ContainElement(r))
	}

	nodesByLoc = map[string][]uint32{
		"Asia":         root.GetNodesByOption("/Location:Asia"),
		"Europe":       root.GetNodesByOption("/Location:Europe"),
		"NorthAmerica": root.GetNodesByOption("/Location:NorthAmerica"),
		"Italy":        root.GetNodesByOption("/Location:Europe/Country:Italy"),
		"Russia":       root.GetNodesByOption("/Location:Europe/Country:Russia"),
	}

	// check if NE filter works
	for _, loc := range []string{"Asia", "Europe", "NorthAmerica"} {
		ss = []Select{
			{Key: "Location", Count: 2},
		}
		fs = []Filter{
			{Key: "Location", F: FilterNE(loc)},
		}

		c = root.FindGraph(nil, SFGroup{Selectors: ss, Filters: fs})
		g.Expect(c).NotTo(BeNil())
		for _, n := range c.Nodelist() {
			g.Expect(nodesByLoc[loc]).NotTo(ContainElement(n))
		}
	}

	// check if EQ filter works
	ss = []Select{
		{Key: "Country", Count: 1},
	}
	fs = []Filter{
		{Key: "Country", F: FilterEQ("Russia")},
	}

	exp, err = newRoot(bucket{"/Location:Europe/Country:Russia/City:Moscow", []uint32{13, 14}})
	g.Expect(err).NotTo(HaveOccurred())

	c = root.FindGraph(nil, SFGroup{Selectors: ss, Filters: fs})
	g.Expect(c).NotTo(BeNil())
	g.Expect(c).To(Equal(&exp))

	buckets = []bucket{
		{"/Location:Asia/Country:Korea", []uint32{1, 3}},
		{"/Location:Asia/Country:China", []uint32{2}},
	}
	exp, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())

	// check if Select.Count works
	ss = []Select{
		{Key: "Location", Count: 1},
		{Key: "Country", Count: 2},
	}
	fs = []Filter{
		{Key: "Location", F: FilterEQ("Asia")},
	}
	c = root.FindGraph(nil, SFGroup{Selectors: ss, Filters: fs})
	g.Expect(c).To(Equal(&exp))

	ss[1].Count = 4
	c = root.FindGraph(nil, SFGroup{Selectors: ss, Filters: fs})
	g.Expect(c).To(BeNil())

	buckets = []bucket{
		{"/Location:NorthAmerica/Country:USA/City:NewYork", []uint32{19, 20}},
		{"/Location:NorthAmerica/Country:Canada", []uint32{21, 22}},
		{"/Location:NorthAmerica/Country:Mexico", []uint32{23, 24}},
	}
	exp, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())

	// check with NotIn filter
	ss = []Select{
		{Key: "Location", Count: 1},
	}
	fs = []Filter{
		{Key: "Location", F: FilterNotIn("Asia", "Europe")},
	}
	c = root.FindGraph(nil, SFGroup{Selectors: ss, Filters: fs})
	g.Expect(c).To(Equal(&exp))
	for _, n := range ns {
		g.Expect(nodesByLoc["NorthAmerica"]).To(ContainElement(n))
	}

	// check with 2 successive filters
	ss = []Select{
		{Key: "Location", Count: 1},
		{Key: "Country", Count: 1},
	}
	fs = []Filter{
		{Key: "Location", F: FilterNotIn("Asia", "Europe")},
		{Key: "Country", F: FilterNotIn("USA", "Canada", "Mexico")},
	}
	c = root.FindGraph(nil, SFGroup{Selectors: ss, Filters: fs})
	g.Expect(c).To(BeNil())

	ss = []Select{
		{Key: "Location", Count: 2},
		{Key: NodesBucket, Count: 3},
	}
	c = root.FindGraph(nil, SFGroup{Selectors: ss})
	g.Expect(c).NotTo(BeNil())

	ss = []Select{
		{Key: "Location", Count: 1},
		{Key: NodesBucket, Count: 6},
	}
	fs = []Filter{
		{Key: "Location", F: FilterEQ("Europe")},
	}
	c = root.FindGraph(nil, SFGroup{Selectors: ss, Filters: fs})
	g.Expect(c).NotTo(BeNil())
	for _, n := range c.Nodelist() {
		g.Expect(nodesByLoc["Europe"]).To(ContainElement(n))
	}

	buckets = []bucket{
		{"/Location:Europe/Country:France/City:Paris", []uint32{6, 7, 8}},
		{"/Location:Europe/Country:Russia/City:Moscow", []uint32{13, 14}},
		{"/Location:NorthAmerica/Country:Canada", []uint32{21, 22}},
	}
	exp, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())

	// multiple selectors
	c = root.FindGraph(nil,
		SFGroup{
			Selectors: []Select{{Key: "City", Count: 1}},
			Filters:   []Filter{{Key: "City", F: FilterEQ("Paris")}},
		},
		SFGroup{
			Selectors: []Select{{Key: "City", Count: 1}},
			Filters:   []Filter{{Key: "City", F: FilterEQ("Moscow")}},
		},
		SFGroup{
			Selectors: []Select{{Key: "Country", Count: 1}},
			Filters:   []Filter{{Key: "Country", F: FilterEQ("Canada")}},
		},
	)
	g.Expect(c).To(Equal(&exp))
}

func TestBucket_FindNodes(t *testing.T) {
	var (
		ns         []uint32
		nodesByLoc map[string][]uint32
		root       Bucket
		ss         []Select
		fs         []Filter
		err        error
	)

	g := NewGomegaWithT(t)

	buckets := []bucket{
		{"/Location:Asia/Country:Korea", []uint32{1, 3}},
		{"/Location:Asia/Country:China", []uint32{2}},
		{"/Location:Asia/Country:Taiwan", []uint32{4, 5}},
		{"/Location:Europe/Country:France", []uint32{6, 7, 8}},
		{"/Location:Europe/Country:Germany", []uint32{9, 10}},
		{"/Location:Europe/Country:Italy", []uint32{11, 12}},
		{"/Location:Europe/Country:Russia", []uint32{13, 14}},
		{"/Location:Europe/Country:Switzerland", []uint32{15, 16}},
		{"/Location:Europe/Country:Spain", []uint32{17, 18}},
		{"/Location:NorthAmerica/Country:USA", []uint32{19, 20}},
		{"/Location:NorthAmerica/Country:Canada", []uint32{21, 22}},
		{"/Location:NorthAmerica/Country:Mexico", []uint32{23, 24}},
	}

	root, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())

	nodesByLoc = map[string][]uint32{
		"Asia":         root.GetNodesByOption("/Location:Asia"),
		"Europe":       root.GetNodesByOption("/Location:Europe"),
		"NorthAmerica": root.GetNodesByOption("/Location:NorthAmerica"),
		"Italy":        root.GetNodesByOption("/Location:Europe/Country:Italy"),
		"Russia":       root.GetNodesByOption("/Location:Europe/Country:Russia"),
	}

	// check if NE filter works
	for _, loc := range []string{"Asia", "Europe", "NorthAmerica"} {
		ss = []Select{
			{Key: "Location", Count: 2},
		}
		fs = []Filter{
			{Key: "Location", F: FilterNE(loc)},
		}
		ns = root.FindNodes(nil, SFGroup{Selectors: ss, Filters: fs})
		g.Expect(ns).NotTo(HaveLen(0))
		for _, n := range ns {
			g.Expect(nodesByLoc[loc]).NotTo(ContainElement(n))
		}
	}

	// check if EQ filter works
	for _, c := range []string{"Italy", "Russia"} {
		ss = []Select{
			{Key: "Country", Count: 1},
		}
		fs = []Filter{
			{Key: "Country", F: FilterEQ(c)},
		}
		ns = root.FindNodes(nil, SFGroup{Selectors: ss, Filters: fs})
		g.Expect(ns).NotTo(HaveLen(0))
		for _, n := range ns {
			g.Expect(nodesByLoc[c]).To(ContainElement(n))
		}
	}

	// check if Select.Count works
	ss = []Select{
		{Key: "Location", Count: 1},
		{Key: "Country", Count: 2},
	}
	fs = []Filter{
		{Key: "Location", F: FilterEQ("Asia")},
	}
	ns = root.FindNodes(nil, SFGroup{Selectors: ss, Filters: fs})
	g.Expect(ns).NotTo(HaveLen(0))

	ss[1].Count = 4
	ns = root.FindNodes(nil, SFGroup{Selectors: ss, Filters: fs})
	g.Expect(ns).To(HaveLen(0))

	// check with NotIn filter
	ss = []Select{
		{Key: "Location", Count: 1},
	}
	fs = []Filter{
		{Key: "Location", F: FilterNotIn("Asia", "Europe")},
	}
	ns = root.FindNodes(nil, SFGroup{Selectors: ss, Filters: fs})
	g.Expect(ns).NotTo(HaveLen(0))
	for _, n := range ns {
		g.Expect(nodesByLoc["NorthAmerica"]).To(ContainElement(n))
	}

	// check with 2 successive filters
	ss = []Select{
		{Key: "Location", Count: 1},
		{Key: "Country", Count: 1},
	}
	fs = []Filter{
		{Key: "Location", F: FilterNotIn("Asia", "Europe")},
		{Key: "Country", F: FilterNotIn("USA", "Canada", "Mexico")},
	}
	ns = root.FindNodes(nil, SFGroup{Selectors: ss, Filters: fs})
	g.Expect(ns).To(HaveLen(0))

	ss = []Select{
		{Key: "Location", Count: 2},
		{Key: NodesBucket, Count: 3},
	}
	ns = root.FindNodes(nil, SFGroup{Selectors: ss})
	g.Expect(ns).To(HaveLen(6))

	ss = []Select{
		{Key: "Location", Count: 1},
		{Key: NodesBucket, Count: 6},
	}
	fs = []Filter{
		{Key: "Location", F: FilterEQ("Europe")},
	}
	ns = root.FindNodes(nil, SFGroup{Selectors: ss, Filters: fs})
	g.Expect(ns).To(HaveLen(6))
	for _, n := range ns {
		g.Expect(nodesByLoc["Europe"]).To(ContainElement(n))
	}

	// consistency test
	ss = []Select{
		{Count: 1, Key: "Country"},
		{Key: NodesBucket, Count: 3},
	}
	fs = []Filter{
		{Key: "Location", F: FilterIn("Asia", "Europe")},
	}
	ns = root.FindNodes(defaultPivot, SFGroup{Selectors: ss, Filters: fs})
	g.Expect(ns).To(HaveLen(3))

	nscopy := root.FindNodes(defaultPivot, SFGroup{Selectors: ss})
	g.Expect(nscopy).To(HaveLen(3))
	g.Expect(ns).To(BeEquivalentTo(nscopy))
}

func TestBucket_NewOption(t *testing.T) {
	var (
		before, after Bucket
		data          []byte
		err           error
	)

	g := NewGomegaWithT(t)

	before = Bucket{}
	err = before.AddBucket("/a:b/c:d", nil)
	g.Expect(err).NotTo(HaveOccurred())

	data, err = before.MarshalBinary()
	g.Expect(err).NotTo(HaveOccurred())
	err = after.UnmarshalBinary(data)
	g.Expect(err).NotTo(HaveOccurred())

	g.Expect(before).To(Equal(after))
}

func TestBucket_MarshalBinaryStress(t *testing.T) {
	var (
		before, after Bucket
		data          []byte
		s             string
	)

	g := NewGomegaWithT(t)

	before, _ = newRoot()
	for i := uint32(1); i < 1000; i++ {
		s += fmt.Sprintf("/k%d:v%d", i, i)
		err := before.AddBucket(s, []uint32{i})
		g.Expect(err).NotTo(HaveOccurred())
	}

	data, err := before.MarshalBinary()
	g.Expect(err).NotTo(HaveOccurred())
	err = after.UnmarshalBinary(data)
	g.Expect(err).NotTo(HaveOccurred())

	g.Expect(before).To(Equal(after))
}

func Benchmark_MarshalStress(b *testing.B) {
	var (
		before, after Bucket
		s             string
	)

	g := NewGomegaWithT(b)

	before, _ = newRoot()
	for i := uint32(1); i < 1000; i++ {
		s += fmt.Sprintf("/k%d:v%d", i, i)
		err := before.AddBucket(s, []uint32{i})
		g.Expect(err).NotTo(HaveOccurred())
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data, _ := before.MarshalBinary()
		err := after.UnmarshalBinary(data)
		if err != nil {
			b.Fatal(err)
			b.FailNow()
		}
	}
}

func TestBucket_BigMap(t *testing.T) {
	var (
		loc, country, city, dc, n int
		total                     uint32
		err                       error
		locLim                    = 10
		countryLim                = 10
		cityLim                   = 10
		dcLim                     = 10
		nLim                      = 100

		ss []Select
		ff []Filter
	)
	g := NewGomegaWithT(t)

	root, _ := newRoot()

	templateTrust := "/Trust:0."
	templateStorage := "/Storage:"

	maxtotal := locLim * countryLim * cityLim * dcLim * nLim
	storagessd := make([]uint32, 0, maxtotal)
	storagemem := make([]uint32, 0, maxtotal)
	storagetape := make([]uint32, 0, maxtotal)
	trust9 := make([]uint32, 0, maxtotal)
	trust8 := make([]uint32, 0, maxtotal)
	trust7 := make([]uint32, 0, maxtotal)
	trust6 := make([]uint32, 0, maxtotal)
	trust5 := make([]uint32, 0, maxtotal)

	start := time.Now()
	for loc = 0; loc < locLim; loc++ {
		loStr := "lo" + strconv.Itoa(loc)
		loB, _ := newRoot()
		for country = 0; country < countryLim; country++ {
			coStr := loStr + "co" + strconv.Itoa(country)
			countryB, _ := newRoot()
			for city = 0; city < cityLim; city++ {
				ciStr := coStr + "ci" + strconv.Itoa(city)
				cityB, _ := newRoot()
				for dc = 0; dc < dcLim; dc++ {
					dcStr := ciStr + "dc" + strconv.Itoa(dc)
					ns := make([]uint32, 0, nLim)
					for n = 0; n < nLim; n++ {
						ns = append(ns, total)
						total++
						switch total % 3 {
						case 0:
							storagessd = append(storagessd, total)
						case 1:
							storagemem = append(storagemem, total)
						case 2:
							storagetape = append(storagetape, total)
						}

						switch total % 5 {
						case 0:
							trust9 = append(trust9, total)
						case 1:
							trust8 = append(trust8, total)
						case 2:
							trust7 = append(trust7, total)
						case 3:
							trust6 = append(trust6, total)
						case 4:
							trust5 = append(trust5, total)
						}
					}
					err = cityB.AddBucket("/DC:"+dcStr, ns)
					g.Expect(err).NotTo(HaveOccurred())
				}
				cityB.Key = "City"
				cityB.Value = ciStr
				countryB.AddChild(cityB)
			}
			countryB.Key = "Country"
			countryB.Value = coStr
			loB.AddChild(countryB)
		}
		loB.Key = "Loc"
		loB.Value = loStr
		root.AddChild(loB)
	}
	err = root.AddBucket(templateStorage+"SSD", storagessd)
	g.Expect(err).NotTo(HaveOccurred())
	err = root.AddBucket(templateStorage+"MEM", storagemem)
	g.Expect(err).NotTo(HaveOccurred())
	err = root.AddBucket(templateStorage+"TAPE", storagetape)
	g.Expect(err).NotTo(HaveOccurred())

	err = root.AddBucket(templateTrust+"9", trust9)
	g.Expect(err).NotTo(HaveOccurred())
	err = root.AddBucket(templateTrust+"8", trust8)
	g.Expect(err).NotTo(HaveOccurred())
	err = root.AddBucket(templateTrust+"7", trust7)
	g.Expect(err).NotTo(HaveOccurred())
	err = root.AddBucket(templateTrust+"6", trust6)
	g.Expect(err).NotTo(HaveOccurred())
	err = root.AddBucket(templateTrust+"5", trust5)
	g.Expect(err).NotTo(HaveOccurred())

	fmt.Println("Map creation time:\t", time.Since(start))

	start = time.Now()
	root.Copy()
	fmt.Println("Map copy time:\t", time.Since(start))

	graph, err := root.MarshalBinary()
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(len(root.children)).To(Equal(locLim + 8))

	newgraph, _ := newRoot()
	err = newgraph.UnmarshalBinary(graph)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(len(newgraph.children)).To(Equal(locLim + 8))

	ss = []Select{
		{Key: "Loc", Count: 1},
		{Key: "Country", Count: 2},
		{Key: NodesBucket, Count: 10},
	}

	ff = []Filter{
		{Key: "Loc", F: FilterEQ("lo5")},
		{Key: "Country", F: FilterAND(FilterNE("lo5co0"), FilterNE("lo5co1"))},
		{Key: "Storage", F: FilterEQ("SSD")},
		{Key: "Trust", F: FilterEQ("0.8")},
	}

	start = time.Now()
	r := root.GetMaxSelection(SFGroup{Selectors: ss, Filters: ff})
	g.Expect(r).NotTo(BeNil())
	z := r.GetSelection(ss, defaultPivot)
	nodes := z.Nodelist()
	fmt.Println("Traverse time:\t\t", time.Since(start))
	g.Expect(len(nodes)).To(Equal(20))

	var testcont []uint32
	for _, b := range root.children {
		if b.Key == "Storage" && b.Value == "SSD" {
			testcont = b.nodes
		}
	}
	for _, node := range nodes {
		g.Expect(testcont).To(ContainElement(node))
	}

	for _, b := range root.children {
		if b.Key == "Trust" && b.Value == "0.8" {
			testcont = b.nodes
		}
	}
	for _, node := range nodes {
		g.Expect(testcont).To(ContainElement(node))
	}
	fmt.Println("Graph size: ", len(graph), " bytes")
}

func TestBucket_ShuffledSelection(t *testing.T) {
	var (
		err             error
		exp, root       Bucket
		r, expr         *Bucket
		buckets         []bucket
		shuffledBuckets []bucket
		ss              []Select
	)
	g := NewGomegaWithT(t)

	buckets = []bucket{
		{"/Location:Asia/Country:Korea", []uint32{1, 3}},
		{"/Location:Asia/Country:China", []uint32{2}},
		{"/Location:Europe/Country:Germany/City:Hamburg", []uint32{25}},
		{"/Location:Europe/Country:Germany/City:Bremen", []uint32{27, 29}},
		{"/Location:Europe/Country:Spain/City:Madrid", []uint32{17, 18}},
		{"/Location:Europe/Country:Spain/City:Barcelona", []uint32{26, 30}},
		{"/Location:NorthAmerica/Country:USA/City:NewYork", []uint32{19, 20}},
	}

	ss = []Select{
		{Key: "City", Count: 3},
		{Key: NodesBucket, Count: 1},
	}

	shuffledBuckets = make([]bucket, len(buckets))
	copy(shuffledBuckets, buckets)
	rand.Shuffle(len(shuffledBuckets), func(i, j int) {
		shuffledBuckets[i], shuffledBuckets[j] = shuffledBuckets[j], shuffledBuckets[i]
	})

	exp, err = newRoot(buckets...)
	g.Expect(err).NotTo(HaveOccurred())

	root, err = newRoot(shuffledBuckets...)
	g.Expect(err).NotTo(HaveOccurred())

	expr = exp.GetSelection(ss, defaultPivot)
	g.Expect(expr).NotTo(BeNil())

	r = root.GetSelection(ss, defaultPivot)
	g.Expect(r).NotTo(BeNil())

	g.Expect(r.nodes).To(Equal(expr.nodes))
}
