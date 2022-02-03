package db

func TablesEqual(t1 Table, t2 Table) bool {
	if len(t1.attrs) != len(t2.attrs) {
		return false
	}
	for i, attr := range t1.attrs {
		if t2.attrs[i] != attr {
			return false
		}
	}
	if t1.Size() != t2.Size() {
		return false
	}
	for i := range t1.Tuples {
		if !TuplesEqual(t1.Tuples[i], t2.Tuples[i]) {
			return false
		}
	}
	return true
}

func TuplesEqual(t1 Tuple, t2 Tuple) bool {
	if len(t1) != len(t2) {
		return false
	}
	for i := range t1 {
		if t1[i] != t2[i] {
			return false
		}
	}
	return true
}

func TablesDeepEqual(tab1 Table, tab2 Table) bool {
	if len(tab1.attrs) != len(tab2.attrs) {
		return false
	}
	for _, attr := range tab1.Attributes() {
		if _, ok := tab2.Position(attr); !ok {
			return false
		}
	}
	for _, attr := range tab2.Attributes() {
		if _, ok := tab1.Position(attr); !ok {
			return false
		}
	}
	if tab1.Size() != tab2.Size() {
		return false
	}
	for _, tup1 := range tab1.Tuples {
		if !containsTuple(tup1, tab1.attrs, &tab2) {
			return false
		}
	}
	for _, tup2 := range tab2.Tuples {
		if !containsTuple(tup2, tab2.attrs, &tab1) {
			return false
		}
	}
	return true
}

func containsTuple(tup Tuple, attrs []string, tab *Table) bool {
	for _, tup2 := range tab.Tuples {
		found := true
		if len(tup) != len(tup2) {
			found = false
			break
		}
		for i := 0; i < len(tup); i++ {
			p2, _ := tab.Position(attrs[i])
			if tup[i] != tup2[p2] {
				found = false
				break
			}
		}
		if found {
			return true
		}
	}
	return false
}

type RelationSchema interface {
	Attributes() []string
	Position(attr string) (pos int, ok bool)
}

func JoinAttrs(rels ...RelationSchema) ([]string, map[string][]RelationSchema) {
	var all []string
	common := make(map[string][]RelationSchema)

	for _, r := range rels {
		for _, a := range r.Attributes() {
			if _, ok := common[a]; !ok {
				common[a] = make([]RelationSchema, 0)
				all = append(all, a)
			}
			common[a] = append(common[a], r)
		}
	}

	return all, common
}

func StatsToRels(stats ...*Statistics) []RelationSchema {
	var rels []RelationSchema
	for _, s := range stats {
		rels = append(rels, s)
	}
	return rels
}

func RelsToStats(rels ...RelationSchema) []*Statistics {
	var stats []*Statistics
	for _, r := range rels {
		stats = append(stats, r.(*Statistics))
	}
	return stats
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
