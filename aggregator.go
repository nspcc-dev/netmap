package netmap

import (
	"sort"
)

type (
	// Aggregator can calculate some value across all netmap
	// such as median, minimum or maximum.
	Aggregator interface {
		Add(float64)
		Compute() float64
		Clear()
	}

	// Normalizer normalizes weight.
	Normalizer interface {
		Normalize(w float64) float64
	}

	meanSumAgg struct {
		sum   float64
		count int
	}

	meanAgg struct {
		mean  float64
		count int
	}

	minAgg struct {
		min float64
	}

	maxAgg struct {
		max float64
	}

	meanIQRAgg struct {
		k   float64
		arr []float64
	}

	reverseMinNorm struct {
		min float64
	}

	maxNorm struct {
		max float64
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
	_ Aggregator = (*meanSumAgg)(nil)
	_ Aggregator = (*meanAgg)(nil)
	_ Aggregator = (*minAgg)(nil)
	_ Aggregator = (*maxAgg)(nil)
	_ Aggregator = (*meanIQRAgg)(nil)

	_ Normalizer = (*reverseMinNorm)(nil)
	_ Normalizer = (*maxNorm)(nil)
	_ Normalizer = (*sigmoidNorm)(nil)
	_ Normalizer = (*constNorm)(nil)
)

// NewMeanSumAgg returns an aggregator which
// computes mean value by keeping total sum.
func NewMeanSumAgg() Aggregator {
	return new(meanSumAgg)
}

// NewMeanAgg returns an aggregator which
// computes mean value by recalculating it on
// every addition.
func NewMeanAgg() Aggregator {
	return new(meanAgg)
}

// NewMinAgg returns an aggregator which
// computes min value.
func NewMinAgg() Aggregator {
	return new(minAgg)
}

// NewMaxAgg returns an aggregator which
// computes max value.
func NewMaxAgg() Aggregator {
	return new(maxAgg)
}

// NewMeanIQRAgg returns an aggregator which
// computes mean value of values from IQR interval.
func NewMeanIQRAgg() Aggregator {
	return new(meanIQRAgg)
}

// NewReverseMinNorm returns a normalizer which
// normalize values in range of 0.0 to 1.0 to a minimum value.
func NewReverseMinNorm(min float64) Normalizer {
	return &reverseMinNorm{min: min}
}

// NewMaxNorm returns a normalizer which
// normalize values in range of 0.0 to 1.0 to a maximum value.
func NewMaxNorm(max float64) Normalizer {
	return &maxNorm{max: max}
}

// NewSigmoidNorm returns a normalizer which
// normalize values in range of 0.0 to 1.0 to a scaled sigmoid.
func NewSigmoidNorm(scale float64) Normalizer {
	return &sigmoidNorm{scale: scale}
}

// NewConstNorm returns a normalizer which
// returns a constant values
func NewConstNorm(value float64) Normalizer {
	return &constNorm{value: value}
}

func (a *meanSumAgg) Add(n float64) {
	a.sum += n
	a.count++
}

func (a *meanSumAgg) Compute() float64 {
	if a.count == 0 {
		return 0
	}
	return a.sum / float64(a.count)
}

func (a *meanSumAgg) Clear() {
	a.sum = 0
	a.count = 0
}

func (a *meanAgg) Add(n float64) {
	c := a.count + 1
	a.mean = a.mean*(float64(a.count)/float64(c)) + n/float64(c)
	a.count++
}

func (a *meanAgg) Compute() float64 {
	return a.mean
}

func (a *meanAgg) Clear() {
	a.count = 0
	a.mean = 0
}

func (a *minAgg) Add(n float64) {
	if a.min == 0 || n < a.min {
		a.min = n
	}
}

func (a *minAgg) Compute() float64 {
	return a.min
}

func (a *minAgg) Clear() {
	a.min = 0
}

func (a *maxAgg) Add(n float64) {
	if n > a.max {
		a.max = n
	}
}

func (a *maxAgg) Compute() float64 {
	return a.max
}

func (a *maxAgg) Clear() {
	a.max = 0
}

func (a *meanIQRAgg) Add(n float64) {
	a.arr = append(a.arr, n)
}

func (a *meanIQRAgg) Compute() float64 {
	l := len(a.arr)
	if l == 0 {
		return 0
	}

	sort.Slice(a.arr, func(i, j int) bool { return a.arr[i] < a.arr[j] })

	var min, max float64
	if l < 4 {
		min, max = a.arr[0], a.arr[l-1]
	} else {
		start, end := l/4, l*3/4-1
		iqr := a.k * (a.arr[end] - a.arr[start])
		min, max = a.arr[start]-iqr, a.arr[end]+iqr
	}

	count := 0
	sum := float64(0)
	for _, e := range a.arr {
		if e >= min && e <= max {
			sum += e
			count++
		}
	}
	return sum / float64(count)
}

func (a *meanIQRAgg) Clear() {
	a.arr = a.arr[:0]
}

func (r *reverseMinNorm) Normalize(w float64) float64 {
	if w == 0 {
		return 0
	}
	return r.min / w
}

func (r *maxNorm) Normalize(w float64) float64 {
	if r.max == 0 {
		return 0
	}
	return w / r.max
}

func (r *sigmoidNorm) Normalize(w float64) float64 {
	if r.scale == 0 {
		return 0
	}
	x := w / r.scale
	return x / (1 + x)
}

func (r *constNorm) Normalize(_ float64) float64 {
	return r.value
}
