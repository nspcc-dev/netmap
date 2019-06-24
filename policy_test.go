package netmap

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type bucket struct {
	name  string
	nodes []uint32
}

type strawBucket struct {
	name  string
	nodes Nodes
}

var defaultPivot = []byte("This is default random data")

func newRoot(bs ...bucket) (b Bucket, err error) {
	for i := range bs {
		n := make(Nodes, 0, len(bs[i].nodes))
		for j := range bs[i].nodes {
			n = append(n, Node{N: bs[i].nodes[j]})
		}
		if err = b.AddBucket(bs[i].name, n); err != nil {
			return
		}
	}
	return
}

func newStrawRoot(bs ...strawBucket) (b Bucket, err error) {
	for i := range bs {
		if err = b.AddBucket(bs[i].name, bs[i].nodes); err != nil {
			return
		}
	}
	return
}

func TestBucket_RuntimeError(t *testing.T) {
	t.Run("slice bounds out of range", func(t *testing.T) {
		buckets := []bucket{
			{"/Location:Europe/Country:Spain/City:Madrid", []uint32{17, 18}},
		}
		root, err := newRoot(buckets...)
		require.NoError(t, err)

		ss := []Select{
			{Key: NodesBucket, Count: 3}, // available only two
		}

		r := root.GetSelection(ss, defaultPivot)
		require.Nil(t, r)
	})

	t.Run("test hrw distribution", func(t *testing.T) {
		buckets := []bucket{
			{"/Location:America/Country:USA/City:NewYork", []uint32{0, 1, 2, 3, 4, 5}},
		}

		ss := []Select{
			{Key: "Location", Count: 1},
			{Key: NodesBucket, Count: 3},
		}

		root, err := newRoot(buckets...)
		require.NoError(t, err)

		r1 := root.GetSelection(ss, []byte{1, 2, 3})
		require.NotNil(t, r1)

		r2 := root.GetSelection(ss, []byte{1, 2, 4})
		require.NotNil(t, r2)

		r3 := root.GetSelection(ss, []byte{1, 2, 5})
		require.NotNil(t, r3)

		require.NotEqual(t, r1.nodes, r2.nodes)
		require.NotEqual(t, r1.nodes, r3.nodes)
		require.NotEqual(t, r2.nodes, r3.nodes)
	})

	t.Run("regression test", func(t *testing.T) {
		buckets := []bucket{
			{"/Location:Europe/Country:Spain/City:Madrid", []uint32{17, 18}},
			{"/Location:Europe/Country:Spain/City:Barcelona", []uint32{16, 19}},
		}
		root, err := newRoot(buckets...)
		require.NoError(t, err)

		ss := []Select{
			{Key: "Location", Count: 1},
			{Key: NodesBucket, Count: 3},
		}

		r := root.GetSelection(ss, defaultPivot)
		require.NotNil(t, r)
		require.Len(t, r.nodes, 3)
	})
}

func TestBucket_IsValid(t *testing.T) {
	var (
		b       Bucket
		buckets []bucket
		err     error
	)

	buckets = []bucket{
		{"/Location:Europe/Country:Germany", []uint32{1, 3}},
		{"/Location:Asia/Country:China", []uint32{2}},
	}
	b, err = newRoot(buckets...)
	require.NoError(t, err)
	require.Truef(t, b.IsValid(), "simple bucket should be valid")

	buckets = []bucket{
		{"/Location:Europe/Country:Germany", []uint32{1, 3}},
		{"/Location:Asia/Country:China", []uint32{1, 2}},
	}
	b, err = newRoot(buckets...)
	require.NoError(t, err)
	require.Falsef(t, b.IsValid(), "different children must not intersect")

	b = Bucket{
		Key:   "Location",
		Value: "Europe",
		nodes: Nodes{{N: 1}, {N: 2}},
		children: []Bucket{
			{
				Key:   "Country",
				Value: "Germany",
				nodes: Nodes{{N: 1}, {N: 2}, {N: 3}},
			},
		},
	}
	require.Falsef(t, b.IsValid(), "parent must contain all child nodes")

	// parent can contain more elements
	b = Bucket{
		Key:   "Location",
		Value: "Europe",
		nodes: Nodes{{N: 1}, {N: 2}, {N: 3}},
		children: []Bucket{
			{
				Key:   "Country",
				Value: "Germany",
				nodes: Nodes{{N: 2}},
			},
		},
	}
	require.Truef(t, b.IsValid(), "parent can contain more nodes")
}

func TestBucket_checkConflicts(t *testing.T) {
	var (
		b1, b2  Bucket
		err     error
		buckets []bucket
	)

	buckets = []bucket{
		{"/Location:Europe/Country:Germany", []uint32{1, 3}},
		{"/Location:Asia/Country:China", []uint32{1}},
	}
	b1, err = newRoot(buckets[:1]...)
	require.NoError(t, err)

	b2, err = newRoot(buckets[1:]...)
	require.NoError(t, err)

	require.True(t, b1.CheckConflicts(b2))
	require.True(t, b2.CheckConflicts(b1))

	buckets = []bucket{
		{"/Location:Europe/Country:Germany", []uint32{1, 3}},
		{"/Location:Europe/Country:Germany", []uint32{2, 3}},
	}
	b1, err = newRoot(buckets[:1]...)
	require.NoError(t, err)

	b2, err = newRoot(buckets[1:]...)
	require.NoError(t, err)

	require.False(t, b1.CheckConflicts(b2))
	require.False(t, b2.CheckConflicts(b1))
}

func TestBucket_Merge(t *testing.T) {
	var (
		b1, b2, exp Bucket
		err         error
		buckets     []bucket
	)

	buckets = []bucket{
		{"/Location:Europe/Country:Germany", []uint32{1, 3}},
		{"/Location:Asia/Country:China", []uint32{2}},
	}
	b1, err = newRoot(buckets[:1]...)
	require.NoError(t, err)

	b2, err = newRoot(buckets[1:]...)
	require.NoError(t, err)

	exp, err = newRoot(buckets...)
	require.NoError(t, err)

	b1.Merge(b2)
	require.Equal(t, exp, b1)

	buckets = []bucket{
		{"/Location:Europe/Country:Germany", []uint32{1, 3}},
		{"/Location:Asia/Country:Korea", []uint32{5}},

		{"/Location:Asia/Country:China", []uint32{2, 6}},
		{"/Location:Europe/Country:Germany", []uint32{3, 4}},
	}
	b1, err = newRoot(buckets[:2]...)
	require.NoError(t, err)

	b2, err = newRoot(buckets[2:]...)
	require.NoError(t, err)

	exp, err = newRoot(buckets...)
	require.NoError(t, err)

	b1.Merge(b2)
	require.Equal(t, exp, b1)
}

func TestBucket_GetSelection(t *testing.T) {
	var (
		err       error
		exp, root Bucket
		r         *Bucket
		buckets   []bucket
		ss        []Select
	)

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
	require.NoError(t, err)

	buckets = []bucket{
		{"/Location:Europe/Country:Germany/City:Hamburg", []uint32{25}},
		{"/Location:Europe/Country:Spain/City:Barcelona", []uint32{26, 30}},
	}

	exp, err = newRoot(buckets...)
	require.NoError(t, err)

	ss = []Select{
		{Key: "Location", Count: 1},
		{Key: "City", Count: 2},
	}
	r = root.GetSelection(ss, defaultPivot)
	require.NotNil(t, r)
	require.Equal(t, r.nodes, exp.nodes)

	buckets = []bucket{
		{"/Location:Europe/Country:Spain/City:Barcelona", []uint32{26, 30}},
		{"/Location:NorthAmerica/Country:USA/City:NewYork", []uint32{19, 20}},
	}
	exp, err = newRoot(buckets...)
	require.NoError(t, err)

	ss = []Select{
		{Key: "Location", Count: 2},
		{Key: "City", Count: 1},
	}
	r = root.GetSelection(ss, defaultPivot)
	require.NotNil(t, r)
	require.Equal(t, r.nodes, exp.nodes)
}

func TestBucket_GetWeightSelection(t *testing.T) {
	var (
		err     error
		root    Bucket
		r       *Bucket
		buckets []strawBucket
		ss      []Select
		nodes   Nodes
	)

	buckets = []strawBucket{
		{"/Location:Asia/Country:Korea", Nodes{{N: 1, W: 1}, {N: 3, W: 3}}},
		{"/Location:Asia/Country:China", Nodes{{N: 2, W: 1}}},
		{"/Location:Europe/Country:Germany/City:Hamburg", Nodes{{N: 25, W: 8}}},
		{"/Location:Europe/Country:Germany/City:Bremen", Nodes{{N: 27, W: 1}, {N: 29, W: 2}}},
		{"/Location:Europe/Country:Spain/City:Madrid", Nodes{{N: 17, W: 2}, {N: 18, W: 1}}},
		{"/Location:Europe/Country:Spain/City:Barcelona", Nodes{{N: 26, W: 1}, {N: 30, W: 10}}},
		{"/Location:NorthAmerica/Country:USA/City:NewYork", Nodes{{N: 19, W: 1}, {N: 20, W: 9}}},
	}

	root, err = newStrawRoot(buckets...)
	require.NoError(t, err)

	nodes = Nodes{{N: 25, W: 8}, {N: 30, W: 10}, {N: 20, W: 9}, {N: 3, W: 3}}

	ss = []Select{
		{Key: NodesBucket, Count: 4},
	}
	r = root.GetSelection(ss, defaultPivot)
	require.NotNil(t, r)
	require.Equal(t, r.Nodelist(), nodes)

	ss = []Select{
		{Key: "Location", Count: 1},
		{Key: "City", Count: 4},
		{Key: NodesBucket, Count: 1},
	}

	nodes = Nodes{{N: 17, W: 2}, {N: 25, W: 8}, {N: 29, W: 2}, {N: 30, W: 10}}
	r = root.GetSelection(ss, defaultPivot)
	require.NotNil(t, r)
	require.Equal(t, r.Nodelist(), nodes)
}

func TestBucket_GetMaxSelection(t *testing.T) {
	var (
		err       error
		exp, root Bucket
		r         *Bucket
		buckets   []bucket
		sbuckets  []strawBucket
		ss        []Select
		fs        []Filter
	)

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
	require.NoError(t, err)

	buckets = []bucket{
		{"/Location:Europe/Country:Germany/City:Berlin", []uint32{9, 10}},
		{"/Location:Europe/Country:Germany/City:Hamburg", []uint32{25}},
		{"/Location:Europe/Country:Germany/City:Bremen", []uint32{27, 29}},
		{"/Location:Europe/Country:Spain/City:Madrid", []uint32{17, 18}},
		{"/Location:Europe/Country:Spain/City:Barcelona", []uint32{26, 30}},
	}
	exp, err = newRoot(buckets...)
	require.NoError(t, err)

	// check if subgraph with simple select without node-filters works
	ss = []Select{{Key: "Country", Count: 1}}
	fs = []Filter{{Key: "Country", F: FilterIn("Germany", "Spain")}}
	r = root.GetMaxSelection(SFGroup{Selectors: ss, Filters: fs})
	require.Equal(t, &exp, r)

	// check if select with count works
	ss = []Select{
		{Key: "Country", Count: 1},
		{Key: "City", Count: 2},
	}
	r = root.GetMaxSelection(SFGroup{Selectors: ss})
	require.Equal(t, &exp, r)

	// check if count on nodes also works
	ss = []Select{
		{Key: "Location", Count: 1},
		{Key: "Country", Count: 1},
		{Key: NodesBucket, Count: 4},
	}
	fs = []Filter{{Key: "Location", F: FilterEQ("Europe")}}
	r = root.GetMaxSelection(SFGroup{Selectors: ss, Filters: fs})
	require.Equal(t, &exp, r)

	buckets = []bucket{
		{"/Location:Europe/Country:Spain/City:Madrid", []uint32{17, 18}},
		{"/Location:Europe/Country:Spain/City:Barcelona", []uint32{26, 30}},
	}
	exp, err = newRoot(buckets...)
	require.NoError(t, err)

	r = root.GetMaxSelection(SFGroup{
		Selectors: ss,
		Filters:   fs,
		Exclude:   []uint32{9, 27, 29},
	})
	require.Equal(t, &exp, r)

	r = root.GetMaxSelection(SFGroup{
		Selectors: ss,
		Filters:   fs,
		Exclude:   []uint32{9, 27, 29, 26},
	})
	require.Nil(t, r)

	buckets = []bucket{
		{"/Location:Europe/Country:Germany/City:Berlin", []uint32{9, 10}},
		{"/Location:Europe/Country:Germany/City:Hamburg", []uint32{25}},
		{"/Location:Europe/Country:Germany/City:Bremen", []uint32{27, 29}},
		{"/Location:Europe/Country:Italy/City:Rome", []uint32{11, 12}},
		{"/Location:Europe/Country:Spain/City:Madrid", []uint32{17, 18}},
		{"/Location:Europe/Country:Spain/City:Barcelona", []uint32{26, 30}},
	}
	exp, err = newRoot(buckets...)
	require.NoError(t, err)

	// check if select with count works
	ss = []Select{
		{Key: "City", Count: 2},
	}
	r = root.GetMaxSelection(SFGroup{Selectors: ss})
	require.Equal(t, &exp, r)

	// check if weights are correctly saved after filter operation
	sbuckets = []strawBucket{
		{"/Location:Europe/Country:Germany/City:Berlin", Nodes{{N: 9, W: 1}, {N: 10, W: 2}}},
		{"/Location:Europe/Country:Germany/City:Hamburg", Nodes{{N: 25, W: 1}}},
		{"/Location:Europe/Country:Germany/City:Bremen", Nodes{{N: 27, W: 1}, {N: 29, W: 2}}},
		{"/Location:Europe/Country:Italy/City:Rome", Nodes{{N: 11, W: 1}, {N: 12, W: 1}}},
		{"/Location:Europe/Country:Spain/City:Madrid", Nodes{{N: 17, W: 1}, {N: 1, W: 18}}},
		{"/Location:Europe/Country:Spain/City:Barcelona", Nodes{{N: 26, W: 1}, {N: 30, W: 1}}},
	}
	root, err = newStrawRoot(sbuckets...)
	require.NoError(t, err)

	sbuckets = []strawBucket{
		{"/Location:Europe/Country:Germany/City:Berlin", Nodes{{N: 9, W: 1}, {N: 10, W: 2}}},
		{"/Location:Europe/Country:Germany/City:Hamburg", Nodes{{N: 25, W: 1}}},
		{"/Location:Europe/Country:Germany/City:Bremen", Nodes{{N: 27, W: 1}, {N: 29, W: 2}}},
	}
	exp, err = newStrawRoot(sbuckets...)
	require.NoError(t, err)

	ss = []Select{
		{Key: NodesBucket, Count: 1},
	}
	fs = []Filter{{Key: "Country", F: FilterEQ("Germany")}}
	r = root.GetMaxSelection(SFGroup{Selectors: ss, Filters: fs})
	require.Equal(t, &exp, r)

}

func TestNetMap_GetNodesByOption(t *testing.T) {
	var (
		fr, ge, eu, root Bucket
	)

	fr = Bucket{
		Key:   "Country",
		Value: "France",
		nodes: Nodes{{}, {N: 1}, {N: 3}},
	}
	ge = Bucket{
		Key:   "Country",
		Value: "Germany",
		nodes: Nodes{{N: 2}, {N: 4}},
	}
	eu = Bucket{
		Key:      "Location",
		Value:    "Europe",
		nodes:    Nodes{{}, {N: 1}, {N: 2}, {N: 3}, {N: 4}},
		children: []Bucket{fr, ge},
	}
	root = Bucket{
		nodes:    Nodes{{N: 0}, {N: 1}, {N: 2}, {N: 3}, {N: 4}, {N: 5}, {N: 6}},
		children: []Bucket{eu},
	}

	n1 := root.GetNodesByOption("/Location:Europe/Country:Germany")
	require.Equal(t, []uint32{2, 4}, n1.Nodes())

	n2 := root.GetNodesByOption("/Location:Europe/Country:Russia")
	require.Len(t, n2.Nodes(), 0)
}

func TestBucket_AddBucket(t *testing.T) {
	var (
		root, nroot Bucket
		err         error
	)

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
	require.NoError(t, err)
	require.Equal(t, root, nroot)

	// we must correctly handle addition of options without existing parent
	nroot, err = newRoot(
		bucket{"/Location:Europe/Country:France", nil},
		bucket{"/Location:Europe/Country:Germany", nil},
	)
	require.NoError(t, err)
	require.Equal(t, root, nroot)

	// nothing should happen if we add an already existing option
	err = nroot.AddBucket("/Location:Europe", nil)
	require.NoError(t, err)

	require.Equal(t, root, nroot)
}

func TestBucket_AddNode(t *testing.T) {
	var (
		nroot Bucket
		ns    Nodes
		err   error
	)

	nroot, err = newRoot(
		bucket{"/Location:Europe/Country:France", []uint32{1, 3}},
		bucket{"/Location:Europe/Country:Germany", []uint32{7}},
	)
	require.NoError(t, err)

	ns = nroot.GetNodesByOption("/Location:Europe/Country:Germany")
	require.Equal(t, []uint32{7}, ns.Nodes())

	ns = nroot.GetNodesByOption("/Location:Europe")
	require.Equal(t, []uint32{1, 3, 7}, ns.Nodes())
}

func TestNetMap_AddNode(t *testing.T) {
	var (
		root Bucket
		err  error
	)

	root, err = newRoot(
		bucket{"/Location:Europe/Country:France", nil},
		bucket{"/Location:Europe/Country:Germany", nil},
	)
	require.NoError(t, err)

	err = root.AddNode(1, "/Location:Europe/Country:France")
	require.NoError(t, err)
	err = root.AddNode(2, "/Location:Europe/Country:France")
	require.NoError(t, err)
	err = root.AddNode(3, "/Location:Europe/Country:Germany")
	require.NoError(t, err)

	ns := root.GetNodesByOption("/Location:Europe/Country:Germany")
	require.Equal(t, []uint32{3}, ns.Nodes())

	ns = root.GetNodesByOption("/Location:Europe")
	require.Equal(t, []uint32{1, 2, 3}, ns.Nodes())
}

func TestBucket_MarshalBinary(t *testing.T) {
	var (
		before, after Bucket
		data          []byte
		err           error
	)

	before, err = newRoot(
		bucket{"/Location:Europe", []uint32{1}},
		bucket{"/Location:Asia", []uint32{2}},
	)
	require.NoError(t, err)

	data, err = before.MarshalBinary()
	require.NoError(t, err)
	err = after.UnmarshalBinary(data)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestBucket_Nodelist(t *testing.T) {
	var (
		nodes   Nodes
		root    Bucket
		buckets []bucket
		err     error
	)

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
	require.NoError(t, err)

	root.nodes = nil

	root.fillNodes()
	nodes = root.Nodelist()
	require.Len(t, nodes, 24)
	for i := uint32(1); i <= 24; i++ {
		require.Contains(t, nodes.Nodes(), i)
	}
}

func TestNetMap_FindGraph(t *testing.T) {
	var (
		nodesByLoc map[string]Nodes
		root, exp  Bucket
		c          *Bucket
		ss         []Select
		fs         []Filter
		err        error
	)

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
	require.NoError(t, err)

	ss = []Select{{Key: NodesBucket, Count: 6}}
	c = root.FindGraph(nil, SFGroup{Selectors: ss})
	require.NotNil(t, c)
	require.Len(t, c.Nodelist(), 6)
	for _, r := range c.Nodelist() {
		require.Contains(t, []uint32{1, 2, 3, 6, 7, 8}, r.N)
	}

	nodesByLoc = map[string]Nodes{
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
		require.NotNil(t, c)
		for _, n := range c.Nodelist() {
			require.NotContains(t, nodesByLoc[loc], n.N)
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
	require.NoError(t, err)

	c = root.FindGraph(nil, SFGroup{Selectors: ss, Filters: fs})
	require.NotNil(t, c)
	require.Equal(t, &exp, c)

	buckets = []bucket{
		{"/Location:Asia/Country:Korea", []uint32{1, 3}},
		{"/Location:Asia/Country:China", []uint32{2}},
	}
	exp, err = newRoot(buckets...)
	require.NoError(t, err)

	// check if Select.Count works
	ss = []Select{
		{Key: "Location", Count: 1},
		{Key: "Country", Count: 2},
	}
	fs = []Filter{
		{Key: "Location", F: FilterEQ("Asia")},
	}
	c = root.FindGraph(nil, SFGroup{Selectors: ss, Filters: fs})
	require.Equal(t, &exp, c)

	ss[1].Count = 4
	c = root.FindGraph(nil, SFGroup{Selectors: ss, Filters: fs})
	require.Nil(t, c)

	buckets = []bucket{
		{"/Location:NorthAmerica/Country:USA/City:NewYork", []uint32{19, 20}},
		{"/Location:NorthAmerica/Country:Canada", []uint32{21, 22}},
		{"/Location:NorthAmerica/Country:Mexico", []uint32{23, 24}},
	}
	exp, err = newRoot(buckets...)
	require.NoError(t, err)

	// check with NotIn filter
	ss = []Select{
		{Key: "Location", Count: 1},
	}
	fs = []Filter{
		{Key: "Location", F: FilterNotIn("Asia", "Europe")},
	}
	c = root.FindGraph(nil, SFGroup{Selectors: ss, Filters: fs})
	require.Equal(t, &exp, c)
	for _, n := range c.Nodelist() {
		require.Contains(t, nodesByLoc["NorthAmerica"], n)
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
	require.Nil(t, c)

	ss = []Select{
		{Key: "Location", Count: 2},
		{Key: NodesBucket, Count: 3},
	}
	c = root.FindGraph(nil, SFGroup{Selectors: ss})
	require.NotNil(t, c)

	ss = []Select{
		{Key: "Location", Count: 1},
		{Key: NodesBucket, Count: 6},
	}
	fs = []Filter{
		{Key: "Location", F: FilterEQ("Europe")},
	}
	c = root.FindGraph(nil, SFGroup{Selectors: ss, Filters: fs})
	require.NotNil(t, c)
	for _, n := range c.Nodelist() {
		require.Contains(t, nodesByLoc["Europe"], n)
	}

	buckets = []bucket{
		{"/Location:Europe/Country:France/City:Paris", []uint32{6, 7, 8}},
		{"/Location:Europe/Country:Russia/City:Moscow", []uint32{13, 14}},
		{"/Location:NorthAmerica/Country:Canada", []uint32{21, 22}},
	}
	exp, err = newRoot(buckets...)
	require.NoError(t, err)

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
	require.Equal(t, &exp, c)
}

func TestBucket_FindNodes(t *testing.T) {
	var (
		ns         Nodes
		nodesByLoc map[string]Nodes
		root       Bucket
		ss         []Select
		fs         []Filter
		err        error
	)

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
	require.NoError(t, err)

	nodesByLoc = map[string]Nodes{
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
		require.NotEmpty(t, ns)
		for _, n := range ns {
			require.NotContains(t, nodesByLoc[loc], n)
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
		require.NotEmpty(t, ns)
		for _, n := range ns {
			require.Contains(t, nodesByLoc[c], n)
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
	require.NotEmpty(t, ns)

	ss[1].Count = 4
	ns = root.FindNodes(nil, SFGroup{Selectors: ss, Filters: fs})
	require.Len(t, ns, 0)

	// check with NotIn filter
	ss = []Select{
		{Key: "Location", Count: 1},
	}
	fs = []Filter{
		{Key: "Location", F: FilterNotIn("Asia", "Europe")},
	}
	ns = root.FindNodes(nil, SFGroup{Selectors: ss, Filters: fs})
	require.NotEmpty(t, ns)
	for _, n := range ns {
		require.Contains(t, nodesByLoc["NorthAmerica"], n)
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
	require.Len(t, ns, 0)

	ss = []Select{
		{Key: "Location", Count: 2},
		{Key: NodesBucket, Count: 3},
	}
	ns = root.FindNodes(nil, SFGroup{Selectors: ss})
	require.Len(t, ns, 6)

	ss = []Select{
		{Key: "Location", Count: 1},
		{Key: NodesBucket, Count: 6},
	}
	fs = []Filter{
		{Key: "Location", F: FilterEQ("Europe")},
	}
	ns = root.FindNodes(nil, SFGroup{Selectors: ss, Filters: fs})
	require.Len(t, ns, 6)
	for _, n := range ns {
		require.Contains(t, nodesByLoc["Europe"], n)
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
	require.Len(t, ns, 3)

	nscopy := root.FindNodes(defaultPivot, SFGroup{Selectors: ss})
	require.Len(t, nscopy, 3)
	require.Equal(t, ns, nscopy)
}

func TestBucket_NewOption(t *testing.T) {
	var (
		before, after Bucket
		data          []byte
		err           error
	)

	before = Bucket{}
	err = before.AddBucket("/a:b/c:d", nil)
	require.NoError(t, err)

	data, err = before.MarshalBinary()
	require.NoError(t, err)
	err = after.UnmarshalBinary(data)
	require.NoError(t, err)

	require.Equal(t, before, after)
}

func TestBucket_MarshalBinaryStress(t *testing.T) {
	var (
		before, after Bucket
		data          []byte
		s             string
	)

	before, _ = newRoot()
	for i := uint32(1); i < 1000; i++ {
		s += fmt.Sprintf("/k%d:v%d", i, i)
		err := before.AddBucket(s, Nodes{{N: i}})
		require.NoError(t, err)
	}

	data, err := before.MarshalBinary()
	require.NoError(t, err)
	err = after.UnmarshalBinary(data)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func Benchmark_MarshalStress(b *testing.B) {
	var (
		before, after Bucket
		s             string
	)

	before, _ = newRoot()
	for i := uint32(1); i < 1000; i++ {
		s += fmt.Sprintf("/k%d:v%d", i, i)
		err := before.AddBucket(s, Nodes{{N: i}})
		require.NoError(b, err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data, _ := before.MarshalBinary()
		err := after.UnmarshalBinary(data)
		if err != nil {
			b.Fatal(err)
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

	root, _ := newRoot()

	templateTrust := "/Trust:0."
	templateStorage := "/Storage:"

	maxTotal := locLim * countryLim * cityLim * dcLim * nLim
	storageSSD := make(Nodes, 0, maxTotal)
	storageMem := make(Nodes, 0, maxTotal)
	storageTape := make(Nodes, 0, maxTotal)
	trust9 := make(Nodes, 0, maxTotal)
	trust8 := make(Nodes, 0, maxTotal)
	trust7 := make(Nodes, 0, maxTotal)
	trust6 := make(Nodes, 0, maxTotal)
	trust5 := make(Nodes, 0, maxTotal)

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
					ns := make(Nodes, 0, nLim)
					for n = 0; n < nLim; n++ {
						ns = append(ns, Node{N: total})
						total++
						switch total % 3 {
						case 0:
							storageSSD = append(storageSSD, Node{N: total})
						case 1:
							storageMem = append(storageMem, Node{N: total})
						case 2:
							storageTape = append(storageTape, Node{N: total})
						}

						switch total % 5 {
						case 0:
							trust9 = append(trust9, Node{N: total})
						case 1:
							trust8 = append(trust8, Node{N: total})
						case 2:
							trust7 = append(trust7, Node{N: total})
						case 3:
							trust6 = append(trust6, Node{N: total})
						case 4:
							trust5 = append(trust5, Node{N: total})
						}
					}
					err = cityB.AddBucket("/DC:"+dcStr, ns)
					require.NoError(t, err)
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
	err = root.AddBucket(templateStorage+"SSD", storageSSD)
	require.NoError(t, err)
	err = root.AddBucket(templateStorage+"MEM", storageMem)
	require.NoError(t, err)
	err = root.AddBucket(templateStorage+"TAPE", storageTape)
	require.NoError(t, err)

	err = root.AddBucket(templateTrust+"9", trust9)
	require.NoError(t, err)
	err = root.AddBucket(templateTrust+"8", trust8)
	require.NoError(t, err)
	err = root.AddBucket(templateTrust+"7", trust7)
	require.NoError(t, err)
	err = root.AddBucket(templateTrust+"6", trust6)
	require.NoError(t, err)
	err = root.AddBucket(templateTrust+"5", trust5)
	require.NoError(t, err)

	t.Logf("Map creation time:\t%s", time.Since(start))

	start = time.Now()
	root.Copy()
	t.Logf("Map copy time:\t%s", time.Since(start))

	graph, err := root.MarshalBinary()
	require.NoError(t, err)
	require.Len(t, root.children, locLim+8)

	newgraph, _ := newRoot()
	err = newgraph.UnmarshalBinary(graph)
	require.NoError(t, err)
	require.Len(t, newgraph.children, locLim+8)

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
	require.NotNil(t, r)
	z := r.GetSelection(ss, defaultPivot)
	nodes := z.Nodelist()
	require.Len(t, nodes, 20)

	t.Logf("Traverse time:\t\t%s", time.Since(start))
	var testcont Nodes
	for _, b := range root.children {
		if b.Key == "Storage" && b.Value == "SSD" {
			testcont = b.nodes
		}
	}
	for _, node := range nodes {
		require.Contains(t, testcont, node)
	}

	for _, b := range root.children {
		if b.Key == "Trust" && b.Value == "0.8" {
			testcont = b.nodes
		}
	}
	for _, node := range nodes {
		require.Contains(t, testcont, node)
	}
	t.Logf("Graph size: %d bytes", len(graph))
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
	require.NoError(t, err)

	root, err = newRoot(shuffledBuckets...)
	require.NoError(t, err)

	expr = exp.GetSelection(ss, defaultPivot)
	require.NotNil(t, expr)

	r = root.GetSelection(ss, defaultPivot)
	require.NotNil(t, r)

	require.Equal(t, r.nodes, expr.nodes)
}
