package decomp

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/cem-okulmus/BalancedGo/lib"
)

// SizeEstimates associates costs to combinations of edges
type SizeEstimates map[uint64]int

func LoadEstimates(path string, graph Graph, encoding map[string]int) SizeEstimates {
	// 1. read the csv file
	csvfile, err := os.Open(path)
	if err != nil {
		panic(fmt.Errorf("can't open %v: %v", path, err))
	}

	// 2. init map
	res := make(SizeEstimates)
	r := csv.NewReader(csvfile)
	r.FieldsPerRecord = -1
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		// 3. put the record into the map
		last := len(record) - 1
		cost, err := strconv.Atoi(record[last])
		if err != nil {
			panic(fmt.Errorf("%v is not an int: %v", record[last], err))
		}
		rec := record[:last]
		comb := make([]int, len(rec))
		for p, s := range rec {
			comb[p] = encoding[s]
		}
		edges := selectEdges(graph, comb)
		res.Put(edges, cost)
	}

	return res
}

/*
func selectEdges(graph Graph, comb []int) lib.Edges {
	var output []lib.Edge
	for _, name := range comb {
		if e, ok := findEdge(graph.Edges, name); ok {
			output = append(output, e)
		} else {
			panic(fmt.Errorf("edge %v missing", name))
		}
	}
	return lib.NewEdges(output)
}

func findEdge(edges lib.Edges, name int) (lib.Edge, bool) {
	for _, e := range edges.Slice() {
		if e.Name == name {
			return e, true
		}
	}
	return lib.Edge{}, false
}
*/

// Put the cost of an edge combination into the map
func (se SizeEstimates) Put(edges lib.Edges, cost int) {
	h := hashNames(edges)
	if _, ok := se[h]; ok {
		panic(fmt.Errorf("cost for %v already present", edges))
	}
	se[h] = cost
}

// Cost of an edge combination
func (se SizeEstimates) Cost(edges lib.Edges) int {
	h := hashNames(edges)
	if c, ok := se[h]; ok {
		return c
	}
	panic(fmt.Errorf("cost for %v not present", edges))
}

/*
func hashNames(edges lib.Edges) uint64 {
	var names []int
	for _, e := range edges.Slice() {
		names = append(names, e.Name)
	}

	var output uint64
	for _, item := range names {
		h := fnv.New64a()
		bs := make([]byte, 4)
		binary.PutVarint(bs, int64(item))
		h.Write(bs)
		output = output ^ h.Sum64()
	}

	return output
}
*/
