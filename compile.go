package netmap

import (
	"fmt"
	"strings"
)

type (
	Descriptor = map[string]uint32

	CompiledBucket struct {
		desc    Descriptor
		data    []CNode
		weights map[uint32]uint64
	}

	CompiledFilter struct {
		Op    Operation
		Key   uint32
		Value interface{}
	}

	CompiledSelect struct {
		Key   uint32
		Count int
	}

	CompiledSFGroup struct {
		Filters   []CompiledFilter
		Selectors []CompiledSelect
	}

	CNode struct {
		Size     int
		Key      uint32
		Value    uint32
		disabled bool
	}
)

const (
	// reserved descriptor keys in compiled map
	nodesDesc uint32 = iota
	minDesc
)

func (cb CompiledBucket) Copy() (rb CompiledBucket) {
	rb.data = make([]CNode, len(cb.data))
	copy(rb.data, cb.data)

	rb.desc = make(Descriptor, len(cb.desc))
	for k, v := range cb.desc {
		rb.desc[k] = v
	}
	return
}

func (cb *CompiledBucket) GetMaxSelection(g CompiledSFGroup) *CompiledBucket {
	for i := range g.Filters {
		cb.applyFilter(g.Filters[i])
	}
	applySelects(cb.data[1:], g.Selectors)
	return cb
}

func (cb *CompiledBucket) Shrink() (rb *CompiledBucket) {
	// probably FIXME copy map
	return &CompiledBucket{
		desc: cb.desc,
		data: shrink(cb.data),
	}
}

func shrink(data []CNode) (r []CNode) {
	if data[0].disabled {
		return nil
	}
	r = []CNode{data[0]}
	count := 1
	for count < r[0].Size {
		size := data[count].Size
		t := shrink(data[count:])
		count += size
		r = append(r, t...)
	}
	return r
}

func (g *SFGroup) Compile(desc Descriptor) (cg CompiledSFGroup) {
	cg.Filters = make([]CompiledFilter, len(g.Filters))
	for i := range g.Filters {
		g.Filters[i].compileTo(desc, &cg.Filters[i])
	}

	cg.Selectors = make([]CompiledSelect, len(g.Selectors))
	for i := range g.Selectors {
		cg.Selectors[i].Key = desc[g.Selectors[i].Key]
		cg.Selectors[i].Count = int(g.Selectors[i].Count)
	}
	return
}

func (b *Bucket) Compile() (cb *CompiledBucket) {
	desc := make(Descriptor)
	_, cb = b.compile(minDesc, desc)
	cb.desc = desc
	return cb
}

func (b *Bucket) compile(c uint32, desc Descriptor) (r uint32, cb *CompiledBucket) {
	var (
		d  uint32
		ok bool
	)

	cb = &CompiledBucket{weights: make(map[uint32]uint64)}
	r = c
	if d, ok = desc[b.Key]; !ok {
		d = r
		desc[b.Key] = d
		r++
	}
	if d, ok = desc[b.Value]; !ok {
		d = r
		desc[b.Value] = d
		r++
	}

	ind := len(cb.data)
	cb.data = append(cb.data, CNode{
		Key:   desc[b.Key],
		Value: desc[b.Value],
	})

	var nb *CompiledBucket
	if len(b.children) == 0 {
		for i := range b.nodes {
			cb.data = append(cb.data, CNode{
				Size:  1,
				Key:   nodesDesc,
				Value: b.nodes[i].N,
			})
			cb.weights[b.nodes[i].N] = b.nodes[i].W
		}
		cb.data[ind].Size = len(cb.data) - ind
		return
	}
	for i := range b.children {
		r, nb = b.children[i].compile(r, desc)
		cb.data = append(cb.data, nb.data...)
		for j := range nb.desc {
			cb.desc[j] = nb.desc[j]
		}
	}
	cb.data[ind].Size = len(cb.data) - ind
	return
}

func (cb *CompiledBucket) Decompile() *Bucket {
	desc := invert(cb.desc)
	_, b := decompile(desc, cb.weights, cb.data)
	return &b
}

func decompile(desc map[uint32]string, weights map[uint32]uint64, data []CNode) (count int, b Bucket) {
	if data[0].Key == nodesDesc {
		for i := range data {
			if data[i].Key != 0 {
				break
			}
			b.nodes = append(b.nodes, Node{
				N: data[i].Value,
				W: weights[data[i].Value],
			})
		}
		return len(b.nodes), b
	}

	b = Bucket{
		Key:   desc[data[0].Key],
		Value: desc[data[0].Value],
	}
	for count = 1; count < data[0].Size; {
		n, c := decompile(desc, weights, data[count:])
		if data[1].Key == nodesDesc {
			b.nodes = append(b.nodes, c.nodes...)
		} else {
			b.children = append(b.children, c)
		}
		count += n
	}
	b.fillNodes()

	return count, b
}

func (cb *CompiledBucket) applyFilter(f CompiledFilter) {
	l := len(cb.data)
	switch f.Op {
	case Operation_EQ:
		val := f.Value.(uint32)
		for i := 0; i < l; {
			if f.Key == cb.data[i].Key {
				if val != cb.data[i].Value {
					cb.data[i].disabled = true
					i += cb.data[i].Size
					continue
				}
			}
			i++
		}
	case Operation_NE:
		val := f.Value.(uint32)
		for i := 0; i < l; {
			if f.Key == cb.data[i].Key {
				if val == cb.data[i].Value {
					cb.data[i].disabled = true
					i += cb.data[i].Size
					continue
				}
			}
			i++
		}
	case Operation_AND:
		val := f.Value.([]uint32)
	loop:
		for i := 0; i < l; {
			if f.Key == cb.data[i].Key {
				for j := range val {
					if cb.data[i].Value != val[j] {
						cb.data[i].disabled = true
						i += cb.data[i].Size
						continue loop
					}
				}
			}
			i++
		}
	}
}

// FIXME perform full compilation, not just 2 levels
func (f Filter) compileTo(desc Descriptor, cf *CompiledFilter) {
	cf.Key = desc[f.Key]
	cf.Op = f.F.Op
	switch cf.Op {
	case Operation_EQ, Operation_NE:
		cf.Value = desc[f.F.GetValue()]
	case Operation_AND, Operation_OR:
		fs := f.F.GetFArgs().Filters
		result := make([]uint32, 0, len(fs))
		for i := range fs {
			result = append(result, desc[fs[i].GetValue()])
		}
		cf.Value = result
	}
}

// applySelects returns number of non-disabled nodes
// corresponding to s[0] and -1 if the selector is empty.
func applySelects(data []CNode, s []CompiledSelect) int {
	if len(s) == 0 {
		return -1
	}

	l := len(data)
	count := 0
	for i := 0; i < l; {
		if data[i].Key != s[0].Key {
			i++
			continue
		}
		if !data[i].disabled {
			n := applySelects(data[i+1:i+data[i].Size], s[1:])
			if n == -1 || n >= s[1].Count {
				count++
			} else {
				data[i].disabled = true
			}
		}
		i += data[i].Size
	}
	return count
}

func (cb CompiledBucket) dump() {
	println(cb.sdump())
}

func invert(desc Descriptor) (result map[uint32]string) {
	result = make(map[uint32]string, len(desc))
	for k, v := range desc {
		result[v] = k
	}
	return
}

func (cb CompiledBucket) sdump() string {
	var s strings.Builder
	desc := invert(cb.desc)
	for _, d := range cb.data {
		s.WriteString(d.sdump(desc))
	}
	return s.String()
}

func (c CNode) sdump(desc map[uint32]string) string {
	if c.Key == 0 {
		return fmt.Sprintf("%s:%d (%d) %t\n",
			desc[c.Key], c.Value, c.Size, c.disabled)
	}
	return fmt.Sprintf("%s:%s (%d) %t\n",
		desc[c.Key], desc[c.Value], c.Size, c.disabled)
}

func (b Bucket) sdump() string {
	var s strings.Builder
	s.WriteString(fmt.Sprintf("%s:%s (%d)\n", b.Key, b.Value, len(b.children)+1))
	if len(b.children) == 0 {
		for i := range b.nodes {
			s.WriteString(fmt.Sprintf(":%d (1)\n", b.nodes[i].N))
		}
		return s.String()
	}
	for _, c := range b.children {
		s.WriteString(c.sdump())
	}
	return s.String()
}
