package netmap

func getNodes(b Bucket, path []Bucket) (nodes Nodes) {
	if len(path) == 0 {
		return b.Nodelist()
	}
	for _, p := range b.Children() {
		if p.Equals(path[0]) {
			return getNodes(p, path[1:])
		}
	}
	return nil
}

func contains(nodes Nodes, n Node) bool {
	for _, i := range nodes {
		if i == n {
			return true
		}
	}
	return false
}

func intersect(a, b Nodes) Nodes {
	if a == nil {
		return b
	}

	var (
		la, lb = len(a), len(b)
		l      = min(la, lb)
		c      = make(Nodes, 0, l)
	)

	for i, j := 0, 0; i < la && j < lb; {
		switch true {
		case a[i].n < b[j].n:
			i++
		case a[i].n > b[j].n:
			j++
		default:
			c = append(c, a[i])
			i++
			j++
		}
	}

	return c
}

func diff(a Nodes, b map[uint32]struct{}) (c Nodes) {
	c = make(Nodes, 0, len(a))
	for _, e := range a {
		if _, ok := b[e.n]; !ok {
			c = append(c, e)
		}
	}
	return
}

func union(a, b Nodes) Nodes {
	if a == nil {
		return b
	} else if b == nil {
		return a
	}

	var (
		la, lb = len(a), len(b)
		l      = la + lb
		c      = make(Nodes, 0, l)
		i, j   int
	)

	for i, j = 0, 0; i < la && j < lb; {
		switch true {
		case a[i].n < b[j].n:
			c = append(c, a[i])
			i++
		case a[i].n > b[j].n:
			c = append(c, b[j])
			j++
		default:
			c = append(c, a[i])
			i++
			j++
		}
	}

	if i == la {
		c = append(c, b[j:]...)
	} else {
		c = append(c, a[i:]...)
	}

	return c
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
