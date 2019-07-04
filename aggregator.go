package netmap

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

	reverseMinNorm struct {
		min float64
	}

	sigmoidNorm struct {
		scale float64
	}

	WeightFunc = func(n Node) float64
)

var (
	_ Aggregator = (*meanCapSumAgg)(nil)
	_ Aggregator = (*meanCapAgg)(nil)
	_ Aggregator = (*minPriceAgg)(nil)

	_ Normalizer = (*reverseMinNorm)(nil)
	_ Normalizer = (*sigmoidNorm)(nil)
)

func CapWeightFunc(n Node) float64 { return float64(n.C) }

// TODO generic solution for arbitrary number of weights
func NewWeightFunc(capNorm, priceNorm Normalizer) WeightFunc {
	return func(n Node) float64 {
		return capNorm.Normalize(float64(n.C)) * priceNorm.Normalize(float64(n.P))
	}
}

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
