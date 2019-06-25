package netmap

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
)

func TestBucket_Compile(t *testing.T) {
	var (
		buckets []bucket
		b       Bucket
		err     error
		f       CompiledFilter
		s       CompiledSelect
	)

	buckets = []bucket{
		{"/Location:Europe/Country:Germany", []uint32{1}},
		{"/Location:Europe/Country:France", []uint32{2}},
		{"/Location:Europe/Country:Spain", []uint32{3}},
		{"/Location:America/Country:USA", []uint32{4}},
		{"/Location:America/Country:Canada", []uint32{5}},
		{"/Location:Asia/Country:China", []uint32{6}},
		{"/Location:Asia/Country:Korea", []uint32{7}},
		{"/Location:Australia/Country:Australia", []uint32{8}},
	}
	b, err = newRoot(buckets...)
	require.NoError(t, err)

	cb := b.Compile()
	cb.dump()

	f = CompiledFilter{
		Op:    Operation_NE,
		Key:   cb.desc["Location"],
		Value: cb.desc["America"],
	}
	cb.applyFilter(f)
	cb.dump()

	spew.Dump(cb.desc)
	s = CompiledSelect{
		Key:   cb.desc["Country"],
		Count: 2,
	}
	cb.applySelects([]CompiledSelect{
		{
			Key:   cb.desc["Location"],
			Count: 2,
		},
		s,
	})
	cb.dump()

	cb.Shrink().dump()
}

func randomBucket() *Bucket {
	b := new(Bucket)
	initTestBucket(b, "kek", 4, 10)
	return b
	buckets := []bucket{
		{"/Location:Europe/Country:Germany", []uint32{1}},
		{"/Location:Europe/Country:France", []uint32{2}},
		{"/Location:Europe/Country:Spain", []uint32{3}},
		{"/Location:America/Country:USA", []uint32{4}},
		{"/Location:America/Country:Canada", []uint32{5}},
		{"/Location:Asia/Country:China", []uint32{6}},
		{"/Location:Asia/Country:Korea", []uint32{7}},
		{"/Location:Australia/Country:Australia", []uint32{8}},
	}

	root, err := newRoot(buckets...)
	if err != nil {
		panic(err)
	}
	return &root
}

func prepareSFGroup() SFGroup {
	f := Filter{Key: "a_a_kek", F: FilterNE("America")}
	return SFGroup{
		Selectors: []Select{
			{Key: "kek", Count: 2},
			{Key: "a_kek", Count: 2},
			{Key: NodesBucket, Count: 3},
		},
		Filters: []Filter{f},
	}
}

func BenchmarkBucket_GetMaxSelection(b *testing.B) {
	root := randomBucket()
	g := prepareSFGroup()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = root.GetMaxSelection(g)
	}
}

func BenchmarkCompiledBucket_GetMaxSelection(b *testing.B) {
	root := randomBucket()
	g := prepareSFGroup()

	cb := root.Compile()
	//cg := g.Compile(cb.desc)

	tmp := cb.Copy()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tmp.GetMaxSelection(g.Compile(cb.desc))
	}
}

func TestCompiledBucket_Decompile(t *testing.T) {
	root := new(Bucket)
	initTestBucket(root, "test", 3, 3)

	c := root.Compile()
	require.NotNil(t, c)
	c.dump()

	r := c.Decompile()
	require.NotNil(t, r)

	require.Equal(t, root, r)
}
