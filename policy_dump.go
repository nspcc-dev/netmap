package netmap

import (
	"io/ioutil"
	"os"
	"strconv"

	"github.com/awalterschulze/gographviz"
	"github.com/pkg/errors"
)

// Graph is short synonym for convinience.
type Graph = *gographviz.Graph

func (b Bucket) dumpTo(g Graph) error {
	var (
		attrsN, attrsE map[string]string
		bname          = escapeName(b.Name())
		err            error
	)

	if err = g.AddNode(g.Name, bname, nil); err != nil {
		return errors.Wrapf(err, "cant add node")
	}

	if len(b.children) == 0 {
		if len(b.nodes) == 0 {
			return nil
		}

		attrsN = map[string]string{"shape": "box"}
		attrsE = map[string]string{"style": "dotted"}
		for _, n := range b.nodes {
			if err = g.AddNode(g.Name, strconv.Itoa(int(n)), attrsN); err != nil {
				return err
			}
			if err = g.AddEdge(bname, strconv.Itoa(int(n)), true, attrsE); err != nil {
				return err
			}
		}
	}

	for _, c := range b.children {
		if err = c.dumpTo(g); err != nil {
			return err
		}
		if err = g.AddEdge(bname, escapeName(c.Name()), true, nil); err != nil {
			return errors.Wrapf(err, "cant add edge")
		}
	}
	return nil
}

// Dump dumps string representation of Bucket in *.dot format to file name.
func (b Bucket) Dump(name string) error {
	s, err := b.Sdump()
	if err != nil {
		return err
	}
	return ioutil.WriteFile(name, []byte(s), os.ModePerm)
}

// Sdump returns string representation of Bucket in *.dot format.
func (b Bucket) Sdump() (string, error) {
	g, err := b.toGraph()
	if err != nil {
		return "", err
	}
	return g.String(), nil
}

// SdumpWithSelection returns string representation of Bucket in *.dot format
// where subgraph b1 is highlighted.
func (b Bucket) SdumpWithSelection(b1 Bucket) (string, error) {
	g, err := b.toGraph()
	if err != nil {
		return "", err
	}
	if err = selectBucket(g, b1); err != nil {
		return "", err
	}
	return g.String(), nil
}

// DumpWithSelection dumps string representation of Bucket in *.dot format
// where subgraph b1 is highlighted to file name.
func (b Bucket) DumpWithSelection(name string, b1 Bucket) error {
	s, err := b.SdumpWithSelection(b1)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(name, []byte(s), os.ModePerm)
}

func selectBucket(g Graph, b Bucket) error {
	var (
		bname = escapeName(b.Name())
		attrs = map[string]string{
			"style": "bold",
			"color": "red",
		}
	)

	if len(b.children) == 0 {
		for _, n := range b.nodes {
			applyAttrs(g, bname, strconv.Itoa(int(n)), attrs)
		}
		return nil
	}
	for _, c := range b.children {
		if err := selectBucket(g, c); err != nil {
			return err
		}
		applyAttrs(g, bname, escapeName(c.Name()), attrs)
	}
	return nil
}

func applyAttrs(g Graph, src, dst string, attrs map[string]string) {
	if val, ok := g.Edges.SrcToDsts[src]; ok {
		if edges, ok := val[dst]; ok {
			for i := range edges {
				for k, v := range attrs {
					edges[i].Attrs[gographviz.Attr(k)] = v
				}
			}
		}
	}
}

func escapeName(name string) string {
	return "\"" + name + "\""
}

func (b Bucket) toGraph() (Graph, error) {
	var (
		mg  = gographviz.NewGraph()
		err error
	)

	if err = mg.SetDir(true); err != nil {
		return nil, err
	}
	if err = mg.SetName("Netmap"); err != nil {
		return nil, err
	}
	if err = b.dumpTo(mg); err != nil {
		return nil, err
	}
	return mg, nil
}
