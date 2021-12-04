package decomp

import (
	"fmt"
	"reflect"

	"github.com/cem-okulmus/BalancedGo/lib"
)

func NewDetKSepGen(hg Graph, k int, edges lib.Edges, oldSep []int) *DetKSeparatorIt {
	verticesCurrent := hg.Vertices()
	conn := lib.Inter(oldSep, verticesCurrent)
	compVertices := lib.Diff(verticesCurrent, oldSep)
	bound := lib.FilterVertices(edges, conn)
	sepIt := &DetKSeparatorIt{
		hg:           hg,
		k:            k,
		gen:          lib.NewCover(k, conn, bound, hg.Edges.Vertices()),
		bound:        bound,
		compVertices: compVertices,
		addEdges:     false,
		iAdd:         -1,
		sep:          lib.Edges{},
		next:         lib.Edges{},
		delivered:    false,
	}
	return sepIt
}

type DetKSeparatorIt struct {
	hg           Graph
	k            int
	gen          lib.Cover
	bound        lib.Edges
	compVertices []int
	addEdges     bool
	iAdd         int

	sep       lib.Edges
	next      lib.Edges
	delivered bool
}

func (s *DetKSeparatorIt) update() {
	if s.addEdges {
		s.iAdd++
		if s.iAdd < s.hg.Edges.Len() {
			s.next = lib.NewEdges(append(s.sep.Slice(), s.hg.Edges.Slice()[s.iAdd]))
			s.delivered = false
			return
		} else {
			s.iAdd = -1
			s.addEdges = false
		}
	}

	for s.gen.HasNext {
		out := s.gen.NextSubset()
		if out == -1 {
			if s.gen.HasNext {
				panic(fmt.Errorf("-1 but hasNext not false"))
			}
			continue
		}

		s.sep = lib.GetSubset(s.bound, s.gen.Subset)
		s.addEdges = false
		s.iAdd = -1
		if len(lib.Inter(s.sep.Vertices(), s.compVertices)) != 0 {
			s.next = s.sep
			s.delivered = false
			return
		}
		s.addEdges = true
		s.iAdd = 0
		if s.k-s.sep.Len() > 0 {
			if s.iAdd >= s.hg.Edges.Len() {
				panic("very weird")
			}
			s.next = lib.NewEdges(append(s.sep.Slice(), s.hg.Edges.Slice()[s.iAdd]))
			s.delivered = false
			return
		}
	}
	s.next = lib.Edges{}
	s.delivered = true
}

func (s *DetKSeparatorIt) HasNext() bool {
	nextEmpty := reflect.DeepEqual(s.next, lib.Edges{})
	if (nextEmpty && !s.delivered) || (!nextEmpty && s.delivered) {
		s.update()
	}
	nextEmpty = reflect.DeepEqual(s.next, lib.Edges{})
	return !nextEmpty && !s.delivered
}

func (s *DetKSeparatorIt) Next() lib.Edges {
	if !s.HasNext() {
		panic(fmt.Errorf("wrong state"))
	}
	s.delivered = true
	return s.next
}
