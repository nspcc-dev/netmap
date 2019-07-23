package netmap

import (
	"sort"
)

type (
	// Aggregator can calculate some value across all netmap
	// such as median, minimum or maximum.
	Aggregator interface {
		Add(Node)
		Compute() float64
	}

	// Normalizer normalizes weight.
	Normalizer interface {
		Normalize(w float64) float64
	}

	meanCapSumAgg struct {
		sum   uint64
		count int
	}

	meanCapAgg struct {
		mean  float64
		count int
	}

	minPriceAgg struct {
		min uint64
	}

	meanPriceIQRAgg struct {
		k   float64
		arr []uint64
	}

	reverseMinNorm struct {
		min float64
	}

	sigmoidNorm struct {
		scale float64
	}

	constNorm struct {
		value float64
	}

	// WeightFunc calculates n's weight.
	WeightFunc = func(n Node) float64
)

var (
	_ Aggregator = (*meanCapSumAgg)(nil)
	_ Aggregator = (*meanCapAgg)(nil)
	_ Aggregator = (*minPriceAgg)(nil)
	_ Aggregator = (*meanPriceIQRAgg)(nil)

	_ Normalizer = (*reverseMinNorm)(nil)
	_ Normalizer = (*sigmoidNorm)(nil)
	_ Normalizer = (*constNorm)(nil)
)

// CapWeightFunc calculates weight which is equal to capacity.
func CapWeightFunc(n Node) float64 { return float64(n.C) }

// NewWeightFunc returns WeightFunc which multiplies normalized
// capacity and price.
// TODO generic solution for arbitrary number of weights
func NewWeightFunc(capNorm, priceNorm Normalizer) WeightFunc {
	return func(n Node) float64 {
		return capNorm.Normalize(float64(n.C)) * priceNorm.Normalize(float64(n.P))
	}
}

func getDefaultWeightFunc(ns Nodes) WeightFunc {
	agg := new(meanCapAgg)
	for i := range ns {
		agg.Add(ns[i])
	}
	// TODO replace constNorm for price with minPriceAgg when ready
	return NewWeightFunc(&sigmoidNorm{agg.Compute()}, &constNorm{1})
}

// Traverse adds all Bucket nodes to a and returns it's argument.
func (b *Bucket) Traverse(a Aggregator) Aggregator {
	for i := range b.nodes {
		a.Add(b.nodes[i])
	}
	return a
}

func (a *meanCapSumAgg) Add(n Node) {
	a.sum += n.C
	a.count++
}

func (a *meanCapSumAgg) Compute() float64 {
	return float64(a.sum) / float64(a.count)
}

func (a *meanCapAgg) Add(n Node) {
	c := a.count + 1
	a.mean = a.mean*(float64(a.count)/float64(c)) + float64(n.C)/float64(c)
	a.count++
}

func (a *meanCapAgg) Compute() float64 {
	return a.mean
}

func (a *minPriceAgg) Add(n Node) {
	if a.min == 0 || n.P < a.min {
		a.min = n.P
	}
}

func (a *minPriceAgg) Compute() float64 {
	return float64(a.min)
}

func (a *meanPriceIQRAgg) Add(n Node) {
	a.arr = append(a.arr, n.P)
}

func (a *meanPriceIQRAgg) Compute() float64 {
	l := len(a.arr)
	if l == 0 {
		return 0
	}

	sort.Slice(a.arr, func(i, j int) bool { return a.arr[i] < a.arr[j] })

	var min, max float64
	if l < 4 {
		min, max = float64(a.arr[0]), float64(a.arr[l-1])
	} else {
		start, end := l/4, l*3/4-1
		iqr := a.k * float64(a.arr[end]-a.arr[start])
		min, max = float64(a.arr[start])-iqr, float64(a.arr[end])+iqr
	}

	count := 0
	sum := float64(0)
	for _, e := range a.arr {
		t := float64(e)
		if t >= min && t <= max {
			sum += float64(t)
			count++
		}
	}
	return sum / float64(count)
}

func (r *reverseMinNorm) Normalize(w float64) float64 {
	if w == 0 {
		return 0
	}
	return r.min / w
}

func (r *sigmoidNorm) Normalize(w float64) float64 {
	x := w / r.scale
	return x / (1 + x)
}

func (r *constNorm) Normalize(_ float64) float64 {
	return r.value
}
