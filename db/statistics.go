package db

import (
	"container/heap"
	"fmt"
	"math"
)

type Statistics struct {
	attrs   []string
	attrPos map[string]int

	Size   int
	Ndv    []int
	Hgrams []Histogram
}

func NewStatistics(attrs []string) *Statistics {
	if len(attrs) <= 0 {
		panic(fmt.Errorf("%v is not valid", attrs))
	}

	var stats Statistics
	attrPos := make(map[string]int)
	for i, v := range attrs {
		attrPos[v] = i
	}
	stats.attrs = attrs
	stats.attrPos = attrPos

	stats.Ndv = make([]int, len(attrs))
	stats.Hgrams = make([]Histogram, len(attrs))
	for i := range stats.Hgrams {
		stats.Hgrams[i] = make(Histogram)
	}
	return &stats
}

// S = \sel_{A=c}(R), c constant
func HgramSelectionSize(r Table, attr string, val string) int {
	if i, ok := r.Position(attr); ok {
		return r.hgrams[i].Frequency(val)
	} else {
		panic(fmt.Errorf("%v not in %v", attr, r))
	}
}

func HgramSemijoinSize(r Table, s Table) int {
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

func HgramJoinSize(tables []Table) int {
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
	}

	return n
}

// S = \sel_{A=c}(R), c constant
func NaiveSelectionSize(r Table, attr string) int {
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

func NaiveSemijoinSize(r Table, s Table) int {
	res := r.Size()
	// TODO
	return res
}

// T(R \join S) = T(R)*T(S) / max(V(R,Y),V(S,Y))
func NaiveJoinSize(tables []Table) int {
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
