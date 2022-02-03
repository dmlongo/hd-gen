package decomp

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"strconv"

	"github.com/cem-okulmus/BalancedGo/lib"
	"github.com/dmlongo/hd-gen/db"
)

// SizeEstimates associates statistics to combinations of edges
type StatisticsDB map[uint64]*db.Statistics

func LoadStatistics(path string, graph Graph, encoding map[string]int) StatisticsDB {
	// 1. read the csv file
	csvfile, err := os.Open(path)
	if err != nil {
		panic(fmt.Errorf("can't open %v: %v", path, err))
	}

	// 2. init map
	res := make(StatisticsDB)
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
		val, err := strconv.Atoi(record[last])
		if err != nil {
			panic(fmt.Errorf("%v is not an int: %v", record[last], err))
		}
		tag := record[0]
		switch tag {
		case "size":
			rec := record[1:last]
			comb := make([]int, len(rec))
			for p, s := range rec {
				comb[p] = encoding[s]
			}
			edges := selectEdges(graph, comb)
			if _, ok := res.Stats(edges); !ok {
				attrs := computeAttrs(graph, comb)
				res.Put(edges, db.NewStatistics(attrs))
			}
			st, _ := res.Stats(edges)
			st.SetSize(val)
		case "ndv":
			tabs := record[1 : last-1]
			col := record[last-1]

			comb := make([]int, len(tabs))
			for p, s := range tabs {
				comb[p] = encoding[s]
			}
			edges := selectEdges(graph, comb)
			v := strconv.Itoa(encoding[col])

			if _, ok := res.Stats(edges); !ok {
				attrs := computeAttrs(graph, comb)
				res.Put(edges, db.NewStatistics(attrs))
			}
			st, _ := res.Stats(edges)
			st.SetNdv(v, val)
		}
	}
	// todo here I can estimate ndv for combinations of tables

	return res
}

func computeAttrs(graph Graph, comb []int) []string {
	var attrs []string
	var attrSet map[int]bool
	edges := selectEdges(graph, comb)
	for _, e := range edges.Slice() {
		for _, v := range e.Vertices {
			if _, found := attrSet[v]; !found {
				attrSet[v] = true
				attrs = append(attrs, strconv.Itoa(v))
			}
		}
	}
	return attrs
}

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

// Put the statistics of an edge combination into the map
func (sdb StatisticsDB) Put(edges lib.Edges, stats *db.Statistics) {
	h := hashNames(edges)
	sdb[h] = stats
}

// Statistics of an edge combination
func (sdb StatisticsDB) Stats(edges lib.Edges) (*db.Statistics, bool) {
	h := hashNames(edges)
	if c, ok := sdb[h]; ok {
		return c, true
	}
	return nil, false
}

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

func StatsFromDB(data db.Database, graph Graph, encoding map[string]int) StatisticsDB {
	res := make(StatisticsDB)
	for tName, tab := range data {
		eName := encoding[tName]
		edge := selectEdges(graph, []int{eName})
		res.Put(edge, tab.Stats)
	}
	return res
}
