package netmap

import (
	"github.com/pkg/errors"
)

type (
	Root struct {
		schema Schema
		b      Bucket
	}

	Schema []string
)

func NewRoot(schema Schema) *Root {
	return &Root{
		schema: schema,
	}
}

func (r Root) GetNodes(prefix ...string) (Nodes, error) {
	if len(prefix) > len(r.schema) {
		return nil, errors.New("too many options")
	}
	return r.b.getNodes(prefix...), nil
}

func (b *Bucket) getNodes(prefix ...string) Nodes {
	if len(prefix) == 0 {
		return b.nodes
	}
	for i := range b.children {
		if b.children[i].Value == prefix[0] {
			return b.children[i].getNodes(prefix[1:]...)
		}
	}
	return nil
}

func (r *Root) AddNode(n Node, opts ...string) error {
	if len(opts) != len(r.schema) {
		return errors.Errorf("invalid options (needed %d)", len(r.schema))
	}
	r.b.addNode2(n, opts...)
	return nil
}

func (b *Bucket) addNode2(n Node, opts ...string) {
	b.nodes = append(b.nodes, n)
	if len(opts) == 0 {
		return
	}

	for i := range b.children {
		if b.children[i].Value == opts[0] {
			b.children[i].addNode2(n, opts[1:]...)
			return
		}
	}

	// new bucket needs to be added
	b.children = append(b.children, Bucket{Value: opts[0]})
	b.children[len(b.children)-1].addNode2(n, opts[1:]...)
}
