package netmap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoot_AddNode(t *testing.T) {
	var (
		schema = []string{"Location", "Country", "City"}
		root   = NewRoot(schema)
		nodes  = Nodes{
			{N: 0}, {N: 1}, {N: 2}, {N: 3}, {N: 4}, {N: 5}, {N: 6},
		}
	)

	require.NoError(t, root.AddNode(nodes[0], "Europe", "France", "Paris"))
	require.NoError(t, root.AddNode(nodes[1], "Europe", "France", "Lyon"))
	require.Error(t, root.AddNode(nodes[2], "Europe", "France"))
	require.NoError(t, root.AddNode(nodes[3], "Europe", "Germany", "Berlin"))
	require.NoError(t, root.AddNode(nodes[4], "Asia", "China", "Bejing"))
	require.NoError(t, root.AddNode(nodes[5], "Asia", "China", "Bejing"))
	require.Error(t, root.AddNode(nodes[6], "Asia", "China", "Bejing", "Last"))

	ns, err := root.GetNodes("Europe")
	require.NoError(t, err)
	require.Len(t, ns, 3)
	require.Contains(t, ns, nodes[0])
	require.Contains(t, ns, nodes[1])
	require.Contains(t, ns, nodes[3])
	require.NotContains(t, ns, nodes[2])

	ns, err = root.GetNodes("Asia", "China")
	require.NoError(t, err)
	require.Len(t, ns, 2)
	require.Contains(t, ns, nodes[4])
	require.Contains(t, ns, nodes[5])

	ns, err = root.GetNodes("Asia", "Korea")
	require.NoError(t, err)
	require.Empty(t, ns)

	ns, err = root.GetNodes("Europe", "France", "Paris", "Centrum")
	require.Error(t, err)
	require.Empty(t, ns)

	ns, err = root.GetNodes()
	require.NoError(t, err)
	require.Len(t, ns, 5)
	require.Contains(t, ns, nodes[0])
	require.Contains(t, ns, nodes[1])
	require.Contains(t, ns, nodes[3])
	require.Contains(t, ns, nodes[4])
	require.Contains(t, ns, nodes[5])
}

func BenchmarkRoot_AddNode(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 5; i < b.N; i++ {
		createRoot(5, 5, 1000)
	}
}

func createRoot(width, depth, count int) {
	// TODO implement
}
