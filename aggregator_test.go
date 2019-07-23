package netmap

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	eps float64 = 0.001
)

func initTestBucket(t *testing.T, b *Bucket) {
	require.Nil(t, b.AddBucket("/opt:first", Nodes{{0, 1, 2}, {2, 3, 2}}))
	require.Nil(t, b.AddBucket("/opt:second/sub:1", Nodes{{1, 2, 3}, {10, 6, 1}}))

	b.fillNodes()
}

func TestNewWeightFunc(t *testing.T) {
	var b Bucket

	initTestBucket(t, &b)

	meanCap := b.Traverse(new(meanCapAgg)).Compute()
	capNorm := &sigmoidNorm{scale: meanCap}

	minPrice := b.Traverse(new(minPriceAgg)).Compute()
	priceNorm := &reverseMinNorm{min: minPrice}

	wf := NewWeightFunc(capNorm, priceNorm)

	nodes := make(Nodes, len(b.nodes))
	copy(nodes, b.nodes)

	expected := Nodes{
		{10, 6, 1},
		{2, 3, 2},
		{1, 2, 3},
		{0, 1, 2},
	}

	sort.Slice(nodes, func(i, j int) bool { return wf(nodes[i]) > wf(nodes[j]) })
	require.Equal(t, expected, nodes)
}

func TestAggregator_Compute(t *testing.T) {
	var (
		b Bucket
		a Aggregator
	)

	initTestBucket(t, &b)

	a = new(meanCapAgg)
	b.Traverse(a)
	require.InEpsilon(t, 3.0, a.Compute(), eps)

	a = new(meanCapSumAgg)
	b.Traverse(a)
	require.InEpsilon(t, 3.0, a.Compute(), eps)

	a = new(minPriceAgg)
	b.Traverse(a)
	require.InEpsilon(t, 1.0, a.Compute(), eps)

	a = new(meanPriceIQRAgg)
	b.Traverse(a)
	require.InEpsilon(t, 2.0, a.Compute(), eps)

	mp := new(meanPriceIQRAgg)
	nodes := []Node{{P: 1}, {P: 1}, {P: 10}, {P: 3}, {P: 5}, {P: 5}, {P: 1}, {P: 100}}
	for i := range nodes {
		mp.Add(nodes[i])
	}

	mp.k = 0.5
	require.InEpsilon(t, 2.666, mp.Compute(), eps)

	mp.k = 1.5
	require.InEpsilon(t, 3.714, mp.Compute(), eps)

	mp.k = 23.0
	require.InEpsilon(t, 3.714, mp.Compute(), eps)

	mp.k = 24.0
	require.InEpsilon(t, 15.75, mp.Compute(), eps)

	mp = new(meanPriceIQRAgg)
	mp.Add(Node{P: 1})
	mp.Add(Node{P: 101})
	require.InEpsilon(t, 51.0, mp.Compute(), eps)
}

func TestSigmoidNorm_Normalize(t *testing.T) {
	t.Run("sigmoid norm must equal to 1/2 at `scale`", func(t *testing.T) {
		norm := &sigmoidNorm{scale: 1}
		require.InEpsilon(t, 0.5, norm.Normalize(1), eps)

		norm = &sigmoidNorm{scale: 10}
		require.InEpsilon(t, 0.5, norm.Normalize(10), eps)
	})

	t.Run("sigmoid norm must be less than 1", func(t *testing.T) {
		norm := &sigmoidNorm{scale: 2}
		require.True(t, norm.Normalize(100) < 1)
		require.True(t, norm.Normalize(math.MaxFloat64) <= 1)
	})

	t.Run("sigmoid norm must be monotonic", func(t *testing.T) {
		norm := &sigmoidNorm{scale: 5}
		for i := 0; i < 5; i++ {
			a, b := rand.Float64(), rand.Float64()
			if b < a {
				a, b = b, a
			}
			require.True(t, norm.Normalize(a) <= norm.Normalize(b))
		}
	})
}

func TestReverseMinNorm_Normalize(t *testing.T) {
	t.Run("reverseMin norm should not panic", func(t *testing.T) {
		norm := &reverseMinNorm{min: 0}
		require.NotPanics(t, func() { norm.Normalize(0) })

		norm = &reverseMinNorm{min: 1}
		require.NotPanics(t, func() { norm.Normalize(0) })
	})

	t.Run("reverseMin norm should equal 1 at min value", func(t *testing.T) {
		norm := &reverseMinNorm{min: 10}
		require.InEpsilon(t, 1.0, norm.Normalize(10), eps)
	})
}
