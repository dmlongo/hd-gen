package main

import (
	"container/heap"
	"fmt"
	"math"

	"github.com/cem-okulmus/BalancedGo/lib"
)

type Evaluator interface {
	Eval(decomp Decomp) int
}

type InformedEvaluator struct {
	db  Database
	e2t map[int]string // edge -> table
}

func (qe InformedEvaluator) Eval(decomp Decomp) int {
	cost := 0
	var n *SearchNode
	dfs := makeSearchTree(&decomp.Root).dfs()
	for len(dfs) > 0 {
		n, dfs = dfs[len(dfs)-1], dfs[:len(dfs)-1]
		cost += qe.evalNode(n)
		for _, child := range n.children {
			cost += qe.evalEdge(n, child)
		}
	}
	return cost
}

func (qe InformedEvaluator) evalNode(n *SearchNode) int {
	if n.size == -1 {
		jTables := qe.toTables(n.sep)
		n.size = hJoinSize(jTables)
	}
	return n.size
}

func (qe InformedEvaluator) evalEdge(par *SearchNode, child *SearchNode) int {
	if par.size == -1 || child.size == -1 {
		panic("negative sizes")
	}
	if par.size == 0 || child.size == 0 {
		return 0
	}

	parTables := qe.toTables(par.sep)
	childTables := qe.toTables(child.sep)
	if len(par.sep.Slice()) == 1 && len(child.sep.Slice()) == 1 {
		return hSemijoinSize(parTables[0], childTables[0])
	} // non funziona, non tengo conto che par puo' avere altri figli

	// size_{child} = sel_{child} * par.size
	// sel_{child} = (expected cardinality of q_{child}) / (prod of q_{child} tables)
	num := child.size * par.size
	den := 1
	for _, t := range childTables {
		den *= t.Size()
	}
	return int(math.Round(float64(num) / float64(den)))
	// I think there are smarter ways to do this
}

func (qe InformedEvaluator) toTables(edges lib.Edges) []Table {
	var tables []Table
	for _, e := range edges.Slice() {
		tabName := qe.e2t[e.Name]
		tables = append(tables, qe.db[tabName])
	}
	return tables
}

// S = \sel_{A=c}(R), c constant
func hSelectionSize(r Table, attr string, val int) int {
	if i, ok := r.Position(attr); ok {
		return r.hgrams[i].Frequency(val)
	} else {
		panic(fmt.Errorf("%v not in %v", attr, r))
	}
}

func hSemijoinSize(r Table, s Table) int {
	if r.Size() == 0 || s.Size() == 0 {
		return 0
	}

	sel := 1.0
	jVars := joinAttrTables([]Table{r, s})
	for attr, rels := range jVars {
		if len(rels) > 1 {
			// rels = r,s
			if d, empty := semijoinSelectivity(attr, r, s); !empty {
				sel *= d
			} else {
				return 0
			}
		}
	}

	return int(math.Round(sel * float64(r.Size())))
}

func semijoinSelectivity(attr string, r Table, s Table) (float64, bool) {
	n := 0
	idx := []int{r.attrPos[attr], s.attrPos[attr]}
	for val, freq := range r.hgrams[idx[0]] {
		if s.hgrams[idx[1]].Frequency(val) > 0 {
			n += freq
		}
	}
	if n == 0 {
		return 0.0, true
	}
	num := float64(n)

	return num / float64(r.Size()), false
}

func hJoinSize(tables []Table) int {
	if len(tables) == 1 {
		return tables[0].Size()
	}

	s := 1
	for _, t := range tables {
		s *= t.Size()
		if s == 0 {
			return 0
		}
	}
	sizes := float64(s)

	sel := 1.0
	jVars := joinAttrTables(tables)
	for attr, rels := range jVars {
		if len(rels) > 1 {
			if d, empty := joinSelectivity(attr, rels); !empty {
				sel *= d
			} else {
				return 0
			}
		}
	}

	return int(math.Round(sel * sizes))
}

// pre: tables are not empty
func joinSelectivity(attr string, tables []Table) (float64, bool) {
	n := joinMatchingTuples(attr, tables)
	if n == 0 {
		return 0.0, true
	}
	num := float64(n)

	d := 1
	for _, t := range tables {
		d *= t.Size()
	}
	den := float64(d)

	return num / den, false
}

//pre: len(tables) >= 2
func joinMatchingTuples(attr string, tables []Table) int {
	idx := make([]int, 0)
	for _, t := range tables { // TODO idx structure not really necessary
		if p, ok := t.Position(attr); ok {
			idx = append(idx, p)
		} else {
			panic(fmt.Errorf("%v not in %v", attr, t))
		}
	}

	n := 0
	for val, freq := range tables[0].hgrams[idx[0]] { // TODO choose the smallest hgram
		temp := freq
		for i := 1; i < len(tables); i++ {
			temp *= tables[i].hgrams[idx[i]].Frequency(val)
		}
		n += temp
		if n == 0 {
			return 0
		}
	}

	return n
}

type EstimateEvaluator struct {
	sizes SizeEstimates
}

func (uqe EstimateEvaluator) Eval(decomp Decomp) int {
	cost := 0
	var n *SearchNode
	dfs := makeSearchTree(&decomp.Root).dfs()
	for len(dfs) > 0 {
		n, dfs = dfs[len(dfs)-1], dfs[:len(dfs)-1]
		cost += uqe.evalNode(n)
		for _, child := range n.children {
			cost += uqe.evalEdge(n, child)
		}
	}
	return cost
}

func (uqe EstimateEvaluator) evalNode(n *SearchNode) int {
	if n.size == -1 {
		n.size = uqe.sizes.Cost(n.sep)
	}
	return n.size
}

func (uqe EstimateEvaluator) evalEdge(par *SearchNode, child *SearchNode) int {
	if par.size == -1 || child.size == -1 {
		panic("negative sizes")
	}
	if par.size == 0 || child.size == 0 {
		return 0
	}

	// size_{child} = sel_{child} * par.size
	// sel_{child} = (expected cardinality of q_{child}) / (prod of q_{child} tables)
	num := child.size * par.size
	den := 1
	for _, t := range child.sep.Slice() {
		den *= uqe.sizes.Cost(lib.NewEdges([]lib.Edge{t}))
	}
	res := int(math.Round(float64(num) / float64(den)))
	par.size = res
	return res
}

// S = \sel_{A=c}(R), c constant
func uSelectionSize(r Table, attr string) int {
	if i, ok := r.Position(attr); ok {
		if r.Size() == 0 {
			return 0
		}
		size := float64(r.Size())
		ndv := float64(r.ndv[i])
		return int(math.Round(size / ndv)) // T(S) = T(R) / V(R,A)
	} else {
		panic(fmt.Errorf("%v not in %v", attr, r))
	}
}

func uSemijoinSize(r Table, s Table) int {
	res := r.Size()
	// TODO
	return res
}

// T(R \join S) = T(R)*T(S) / max(V(R,Y),V(S,Y))
func uJoinSize(tables []Table) int {
	n := 1
	for _, t := range tables {
		n *= t.Size()
		if n == 0 {
			return 0
		}
	}
	num := float64(n)

	d := 1
	jVars := joinAttrTables(tables)
	for attr, rels := range jVars {
		if len(rels) > 1 {
			d *= max(rels, attr, len(rels))
		}
	}
	den := float64(d)

	return int(math.Round(num / den))
}

func joinAttrTables(tables []Table) map[string][]Table {
	jVars := make(map[string][]Table)
	for _, t := range tables {
		for _, a := range t.attrs {
			if _, ok := jVars[a]; !ok {
				jVars[a] = make([]Table, 0)
			}
			jVars[a] = append(jVars[a], t)
		}
	}
	return jVars
}

func max(rels []Table, attr string, k int) int {
	var h IntHeap
	heap.Init(&h)
	for _, r := range rels {
		if i, ok := r.Position(attr); ok {
			heap.Push(&h, r.ndv[i])
		} else {
			panic(fmt.Errorf("%v not in %v", attr, r))
		}
	}
	res := 1
	for i := 0; i < k-1; i++ {
		res *= h[i]
	}
	return res
}

// An IntHeap is a min-heap of ints.
type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] > h[j] } // increasing order
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
