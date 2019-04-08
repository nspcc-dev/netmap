package netmap

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"
	"strings"

	"github.com/nspcc-dev/hrw"
	"github.com/pkg/errors"
)

const (
	// Separator separates key:value pairs in string representation of options.
	Separator = "/"

	// NodesBucket is the name for optionless bucket containing only nodes.
	NodesBucket = "Node"
)

type (
	// Policy specifies parameters for storage selection.
	Policy struct {
		Size       int64
		ReplFactor int
		NodeCount  int
	}

	// Bucket represents netmap as graph.
	Bucket struct {
		Key      string
		Value    string
		nodes    Uint32Slice
		children []Bucket
	}

	// Uint32Slice is generic type for more convenient sorting.
	Uint32Slice []uint32

	// FilterFunc is generic type for filtering function on nodes.
	FilterFunc func([]uint32) []uint32
)

func (p Uint32Slice) Len() int           { return len(p) }
func (p Uint32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Hash uses murmur3 hash to return uint64
func (b Bucket) Hash() uint64 {
	return hrw.Hash([]byte(b.Key + b.Value))
}

// FindGraph returns random subgraph, corresponding to specified placement rule.
func (b *Bucket) FindGraph(pivot []byte, ss ...SFGroup) (c *Bucket) {
	var g *Bucket

	c = &Bucket{Key: b.Key, Value: b.Value}
	for _, s := range ss {
		if g = b.findGraph(pivot, s); g == nil {
			return nil
		}
		c.Merge(*g)
	}
	return
}

func (b *Bucket) findGraph(pivot []byte, s SFGroup) (c *Bucket) {
	if c = b.GetMaxSelection(s); c != nil {
		return c.GetSelection(s.Selectors, pivot)
	}
	return
}

// FindNodes returns list of nodes, corresponding to specified placement rule.
func (b *Bucket) FindNodes(pivot []byte, ss ...SFGroup) (nodes []uint32) {
	for _, s := range ss {
		nodes = merge(nodes, b.findNodes(pivot, s))
	}
	return
}

func (b *Bucket) findNodes(pivot []byte, s SFGroup) []uint32 {
	var c *Bucket

	if c = b.GetMaxSelection(s); c != nil {
		if c = c.GetSelection(s.Selectors, pivot); c != nil {
			return c.Nodelist()
		}
	}
	return nil
}

// Copy returns deep copy of Bucket.
func (b Bucket) Copy() Bucket {
	var bc = Bucket{
		Key:   b.Key,
		Value: b.Value,
	}
	if b.nodes != nil {
		bc.nodes = make(Uint32Slice, len(b.nodes))
		copy(bc.nodes, b.nodes)
	}
	if b.children != nil {
		bc.children = make([]Bucket, 0, len(b.children))
		for i := 0; i < len(b.children); i++ {
			bc.children = append(bc.children, b.children[i].Copy())
		}
	}

	return bc
}

// IsValid checks if bucket is well-formed:
// - all nodes contained in sub-bucket must belong to this
// - there must be no nodes belonging to 2 buckets
func (b Bucket) IsValid() bool {
	var (
		ns    Uint32Slice
		nodes = make(Uint32Slice, 0, len(b.nodes))
	)

	if len(b.children) == 0 {
		return true
	}

	for _, c := range b.children {
		if !c.IsValid() {
			return false
		}
		nodes = append(nodes, c.nodes...)
	}

	sort.Sort(nodes)
	ns = intersect(nodes, b.nodes)
	return len(nodes) == len(ns)
}

func (b Bucket) findForbidden(fs []Filter) (forbidden []uint32) {
	// if root does not satisfy any filter it must be forbidden
	for _, f := range fs {
		if b.Key == f.Key && !f.Check(b) {
			return b.nodes
		}
	}

	for _, c := range b.children {
		forbidden = union(forbidden, c.findForbidden(fs))
	}
	return
}

// filterSubtree returns Bucket which contains only nodes,
// satisfying specified filter.
// If Bucket contains 0 nodes, nil is returned.
func (b Bucket) filterSubtree(filter FilterFunc) *Bucket {
	var (
		root Bucket
		r    *Bucket
	)

	root.Key = b.Key
	root.Value = b.Value
	if len(b.children) == 0 {
		if filter != nil {
			root.nodes = filter(b.nodes)
		} else {
			root.nodes = b.nodes
		}
		if len(root.nodes) != 0 {
			return &root
		}
		return nil
	}

	for _, c := range b.children {
		if r = c.filterSubtree(filter); r != nil {
			root.nodes = merge(root.nodes, r.nodes)
			root.children = append(root.children, *r)
		}
	}
	if len(root.nodes) > 0 {
		sort.Sort(root.nodes)
		return &root
	}
	return nil
}

func (b Bucket) getMaxSelection(ss []Select, filter FilterFunc) (*Bucket, uint32) {
	return b.getMaxSelectionC(ss, filter, true)
}

func (b Bucket) getMaxSelectionC(ss []Select, filter FilterFunc, cut bool) (*Bucket, uint32) {
	var (
		root     Bucket
		r        *Bucket
		sel      []Select
		count, n uint32
		cutc     bool
	)

	if len(ss) == 0 || ss[0].Key == NodesBucket {
		if r = b.filterSubtree(filter); r != nil {
			if count = uint32(len(r.nodes)); len(ss) == 0 || ss[0].Count <= count {
				return r, count
			}
		}
		return nil, 0
	}

	root.Key = b.Key
	root.Value = b.Value
	for _, c := range b.children {
		sel = ss
		if cutc = c.Key == ss[0].Key; cutc {
			sel = ss[1:]
		}
		if r, n = c.getMaxSelectionC(sel, filter, cutc); r != nil {
			root.children = append(root.children, *r)
			root.nodes = append(root.nodes, r.Nodelist()...)
			if cutc {
				count++
			} else {
				count += n
			}
		}
	}

	if (!cut && count != 0) || count >= ss[0].Count {
		sort.Sort(root.nodes)
		return &root, count

	}
	return nil, 0
}

// GetMaxSelection returns 'maximal container' -- subgraph which contains
// any other subgraph satisfying specified selects and filters.
func (b Bucket) GetMaxSelection(s SFGroup) (r *Bucket) {
	var (
		forbidden = b.findForbidden(s.Filters)
		excludes  = make(map[uint32]struct{}, len(forbidden)+len(s.Exclude))
	)

	for _, c := range forbidden {
		excludes[c] = struct{}{}
	}
	for _, c := range s.Exclude {
		excludes[c] = struct{}{}
	}

	r, _ = b.getMaxSelection(s.Selectors, func(nodes []uint32) []uint32 {
		return diff(nodes, excludes)
	})
	return
}

// GetSelection returns subgraph, satisfying specified selections.
// It is assumed that all filters were already applied.
func (b Bucket) GetSelection(ss []Select, pivot []byte) *Bucket {
	var (
		pivotHash uint64
		root      = Bucket{Key: b.Key, Value: b.Value}
		r         *Bucket
		count, c  int
		cs        []Bucket
	)
	if len(pivot) != 0 {
		pivotHash = hrw.Hash(pivot)
	}

	if len(ss) == 0 {
		root.nodes = b.nodes
		root.children = b.children
		return &root
	}

	count = int(ss[0].Count)
	if ss[0].Key == NodesBucket {
		if len(b.nodes) < count {
			return nil
		}

		root.nodes = make(Uint32Slice, len(b.nodes))
		copy(root.nodes, b.nodes)
		if len(pivot) != 0 {
			hrw.SortSliceByValue(root.nodes, pivotHash)
		}
		root.nodes = root.nodes[:count]
		return &root
	}

	cs = getChildrenByKey(b, ss[0])
	if len(pivot) != 0 {
		hrw.SortSliceByValue(cs, pivotHash)
	}
	for i := 0; i < len(cs); i++ {
		if r = cs[i].GetSelection(ss[1:], pivot); r != nil {
			root.Merge(*b.combine(r))
			if c++; c == count {
				return &root
			}
		}
	}
	return nil
}

func (b Bucket) combine(b1 *Bucket) *Bucket {
	if b.Equals(*b1) {
		return b1
	}

	var r *Bucket
	for _, c := range b.children {
		if r = c.combine(b1); r != nil {
			return &Bucket{
				Key:      b.Key,
				Value:    b.Value,
				nodes:    r.nodes,
				children: []Bucket{*r},
			}
		}
	}
	return nil
}

// CheckConflicts checks if b1 is ready to merge with b.
// Conflict is a situation, when node has different values for the same option
// in b and b1.
func (b Bucket) CheckConflicts(b1 Bucket) bool {
	for _, n := range b1.nodes {
		if !contains(b.nodes, n) {
			continue
		}
		for _, c := range b.children {
			check := false
			if contains(c.nodes, n) {
				for _, c1 := range b1.children {
					if contains(c1.nodes, n) && (c.Key != c1.Key || c.Value != c1.Value) {
						return true
					}
					if c.Key == c1.Key && c.Value == c1.Value && !check && c.CheckConflicts(c1) {
						return true
					}
					check = true
				}
			}
		}
	}
	return false
}

// Merge merges b1 into b assuming there are no conflicts.
func (b *Bucket) Merge(b1 Bucket) {
	b.nodes = merge(b.nodes, b1.nodes)

loop:
	for _, c1 := range b1.children {
		for i := range b.children {
			if b.children[i].Equals(c1) {
				b.children[i].Merge(c1)
				continue loop
			}
		}
		b.children = append(b.children, c1)
	}
	sort.Sort(b.nodes)
}

// UpdateIndices is auxiliary function used to update
// indices of all nodes according to tr.
func (b *Bucket) UpdateIndices(tr map[uint32]uint32) Bucket {
	var (
		children = make([]Bucket, 0, len(b.children))
		nodes    = make(Uint32Slice, 0, len(b.nodes))
	)

	for i := range b.children {
		children = append(children, b.children[i].UpdateIndices(tr))
	}
	for i := range b.nodes {
		nodes = append(nodes, tr[b.nodes[i]])
	}
	sort.Sort(nodes)

	return Bucket{
		Key:      b.Key,
		Value:    b.Value,
		children: children,
		nodes:    nodes,
	}
}

func getChildrenByKey(b Bucket, s Select) []Bucket {
	buckets := make([]Bucket, 0, 10)
	for _, c := range b.children {
		if s.Key == c.Key {
			buckets = append(buckets, c)
		} else {
			buckets = append(buckets, getChildrenByKey(c, s)...)
		}
	}
	return buckets
}

// Writes Bucket with this byte structure
// [lnName][Name][lnNodes][Node1]...[NodeN][lnSubprops][sub1]...[subN]
func (b Bucket) Write(w io.Writer) error {
	var err error

	// writing name
	if err = binary.Write(w, binary.BigEndian, int32(len(b.Key)+len(b.Value)+1)); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, []byte(b.Name())); err != nil {
		return err
	}

	// writing nodes
	if err = binary.Write(w, binary.BigEndian, int32(len(b.nodes))); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, b.nodes); err != nil {
		return err
	}

	if err = binary.Write(w, binary.BigEndian, int32(len(b.children))); err != nil {
		return err
	}
	for i := range b.children {
		if err = b.children[i].Write(w); err != nil {
			return err
		}
	}

	return nil
}

// Read reads Bucket in serialized form:
// [lnName][Name][lnNodes][Node1]...[NodeN][lnSubprops][sub1]...[subN]
func (b *Bucket) Read(r io.Reader) error {
	var ln int32
	var err error
	if err = binary.Read(r, binary.BigEndian, &ln); err != nil {
		return err
	}
	name := make([]byte, ln)
	lnE, err := r.Read(name)
	if err != nil {
		return err
	}
	if int32(lnE) != ln {
		return errors.New("unmarshaller error: cannot read name")
	}

	b.Key, b.Value, _ = splitKV(string(name))

	// reading node list
	if err = binary.Read(r, binary.BigEndian, &ln); err != nil {
		return err
	}
	if ln > 0 {
		b.nodes = make([]uint32, ln)
		if err = binary.Read(r, binary.BigEndian, &b.nodes); err != nil {
			return err
		}
	}

	if err = binary.Read(r, binary.BigEndian, &ln); err != nil {
		return err
	}
	if ln > 0 {
		b.children = make([]Bucket, ln)
		for i := range b.children {
			if err = b.children[i].Read(r); err != nil {
				return err
			}
		}
	}

	return nil
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (b Bucket) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := b.Write(buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (b *Bucket) UnmarshalBinary(data []byte) (err error) {
	buf := bytes.NewBuffer(data)
	if err = b.Read(buf); err == io.EOF {
		return nil
	}
	return
}

// Name return b's short string identifier.
func (b Bucket) Name() string {
	return b.Key + ":" + b.Value
}

func (b *Bucket) fillNodes() {
	r := b.nodes
	for _, c := range b.children {
		c.fillNodes()
		r = merge(r, c.Nodelist())
	}
	b.nodes = r
}

// Nodelist returns slice of nodes belonging to b.
func (b Bucket) Nodelist() (r []uint32) {
	if b.nodes != nil || len(b.children) == 0 {
		return b.nodes
	}

	for _, c := range b.children {
		r = merge(r, c.Nodelist())
	}
	return
}

// Children returns array of subbuckets of b.
func (b Bucket) Children() []Bucket {
	return b.children
}

// AddNode adds node n with options opts to b.
func (b *Bucket) AddNode(n uint32, opts ...string) error {
	for _, o := range opts {
		if err := b.AddBucket(o, []uint32{n}); err != nil {
			return err
		}
	}
	return nil
}

func splitKV(s string) (string, string, error) {
	kv := strings.SplitN(s, ":", 2)
	if len(kv) != 2 {
		return "", "", errors.New("wrong format")
	}
	return kv[0], kv[1], nil
}

// GetNodesByOption returns list of nodes possessing specified options.
func (b Bucket) GetNodesByOption(opts ...string) []uint32 {
	var nodes []uint32
	for _, opt := range opts {
		nodes = intersect(nodes, getNodes(b, splitProps(opt[1:])))
	}
	return nodes
}

func (b *Bucket) addNodes(bs []Bucket, n []uint32) error {
	b.nodes = merge(b.nodes, n)
	if len(bs) == 0 {
		return nil
	}

	for i := range b.children {
		if bs[0].Equals(b.children[i]) {
			return b.children[i].addNodes(bs[1:], n)
		}
	}
	b.children = append(b.children, makeTreeProps(bs, n))
	return nil
}

// AddBucket add bucket corresponding to option o with nodes n as subbucket to b.
func (b *Bucket) AddBucket(o string, n []uint32) error {
	if o != Separator && (!strings.HasPrefix(o, Separator) || strings.HasSuffix(o, Separator)) {
		return errors.Errorf("must start and not end with '%s'", Separator)
	}

	return b.addNodes(splitProps(o[1:]), n)
}

// AddChild adds c as direct child to b.
func (b *Bucket) AddChild(c Bucket) {
	b.nodes = merge(b.nodes, c.nodes)
	b.children = append(b.children, c)
}

func splitProps(o string) []Bucket {
	ss := strings.Split(o, Separator)
	props := make([]Bucket, 0, 10)
	for _, s := range ss {
		k, v, _ := splitKV(s)
		props = append(props, Bucket{Key: k, Value: v})
	}
	return props
}

func merge(a, b []uint32) []uint32 {
	if a == nil {
		return b
	}

	la, lb := len(a), len(b)
	c := make([]uint32, 0, la+lb)
loop:
	for i, j := 0, 0; i < la || j < lb; {
		switch true {
		case i == la:
			c = append(c, b[j:]...)
			break loop
		case j == lb:
			c = append(c, a[i:]...)
			break loop
		case a[i] < b[j]:
			c = append(c, a[i])
			i++
		case a[i] > b[j]:
			c = append(c, b[j])
			j++
		default:
			c = append(c, a[i])
			i++
			j++
		}
	}

	return c
}

func makeTreeProps(bs []Bucket, n []uint32) Bucket {
	bs[0].nodes = n
	for i := len(bs) - 1; i > 0; i-- {
		bs[i].nodes = n
		bs[i-1].children = []Bucket{bs[i]}
	}
	return bs[0]
}

// Equals checks if b and b1 represent the same Bucket (excluding contained nodes).
func (b Bucket) Equals(b1 Bucket) bool {
	return b.Key == b1.Key && b.Value == b1.Value
}
