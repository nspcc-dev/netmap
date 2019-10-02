package netmap

type (
	// AggregatorFactory is a Factory for a specific Aggregator
	AggregatorFactory struct {
		New func() Aggregator
	}
)

// CapWeightFunc calculates weight which is equal to capacity.
func CapWeightFunc(n Node) float64 { return float64(n.C) }

// PriceWeightFunc calculates weight which is equal to price.
func PriceWeightFunc(n Node) float64 { return float64(n.P) }

// NewWeightFunc returns WeightFunc which multiplies normalized
// capacity and price.
// TODO generic solution for arbitrary number of weights
func NewWeightFunc(capNorm, priceNorm Normalizer) WeightFunc {
	return func(n Node) float64 {
		return capNorm.Normalize(float64(n.C)) * priceNorm.Normalize(float64(n.P))
	}
}

func getDefaultWeightFunc(ns Nodes) WeightFunc {
	mean := new(meanAgg)
	min := new(minAgg)
	for i := range ns {
		mean.Add(float64(ns[i].C))
		min.Add(float64(ns[i].P))
	}
	return NewWeightFunc(&sigmoidNorm{mean.Compute()}, &reverseMinNorm{min.Compute()})
}

// Traverse adds all Bucket nodes to a and returns it's argument.
func (b *Bucket) Traverse(a Aggregator, wf WeightFunc) Aggregator {
	for i := range b.nodes {
		a.Add(wf(b.nodes[i]))
	}
	return a
}

// TraverseTree computes weight for every Bucket and all of its children.
func (b *Bucket) TraverseTree(af AggregatorFactory, wf WeightFunc) {
	a := af.New()
	for i := range b.nodes {
		a.Add(wf(b.nodes[i]))
	}
	b.weight = a.Compute()

	for i := range b.children {
		b.children[i].TraverseTree(af, wf)
	}
}
