package main

import (
	"fmt"
	"reflect"

	"github.com/cem-okulmus/BalancedGo/lib"
)

type DetKStreamer struct {
	K     int
	Graph lib.Graph
	sTree SearchTree

	cache lib.Cache
}

func (d *DetKStreamer) Stream(stop <-chan bool) <-chan Decomp {
	out := make(chan Decomp)
	go func() {
		defer close(out)

		d.cache.Init()
		if d.decompose(d.Graph, []int{}) {
			select {
			case out <- d.buildDecomp():
			case <-stop:
				return
			}
		}
		for d.advance() {
			select {
			case out <- d.buildDecomp():
			case <-stop:
				return
			}
		}
	}()
	return out
}

func (d *DetKStreamer) buildDecomp() Decomp {
	return Decomp{Graph: d.Graph, Root: makeDecomp(d.sTree.root)}
}

func makeDecomp(s *SearchNode) lib.Node {
	n := lib.Node{Bag: s.bag, Cover: s.sep}
	var subtrees []lib.Node
	for _, c := range s.children {
		subtrees = append(subtrees, makeDecomp(c))
	}
	n.Children = subtrees
	return n
}

func (d *DetKStreamer) decompose(H Graph, oldSep []int) bool {
	sepGen := makeSepGen(H, d.K, d.Graph.Edges, oldSep)
	n := d.sTree.makeChild(H, sepGen)
	n.extVerts = append(H.Vertices(), oldSep...)
	found := false
	for n.sepGen.HasNext() {
		n.sep = n.sepGen.Next()
		n.bag = lib.Inter(n.sep.Vertices(), n.extVerts)
		n.myComps, _, _ = H.GetComponents(n.sep)
		if len(n.myComps) == 0 {
			found = true
			break
		}
		allSubDecomp := true
		for _, Hc := range n.myComps {
			allSubDecomp = d.decompose(Hc, n.bag)
			if !allSubDecomp {
				break
			}
		}
		if allSubDecomp {
			found = true
			break
		}
	}
	if found {
		d.sTree.moveUp()
	} else {
		d.sTree.removeChild()
	}
	return found
}

func (d *DetKStreamer) advance() bool {
	found := false
	dfs := d.sTree.dfs()
	for len(dfs) > 0 {
		d.sTree.curr, dfs = dfs[len(dfs)-1], dfs[:len(dfs)-1]
		n := d.sTree.curr
		found = false
		for n.sepGen.HasNext() {
			n.sep = n.sepGen.Next()
			n.bag = lib.Inter(n.sep.Vertices(), n.extVerts)
			n.myComps, _, _ = n.H.GetComponents(n.sep)
			if len(n.myComps) == 0 {
				found = true
				break
			}
			allSubDecomp := true
			for _, Hc := range n.myComps {
				allSubDecomp = d.decompose(Hc, n.bag)
				if !allSubDecomp {
					break
				}
			}
			if allSubDecomp {
				found = true
				break
			}
		}
		if found {
			d.sTree.moveUp()
			par := d.sTree.curr
			if par != nil {
				for i := len(par.children); i < len(par.myComps); i++ {
					Hc := par.myComps[i]
					if !d.decompose(Hc, par.bag) {
						panic(fmt.Errorf("one decomposition should exist"))
					}
				}
			}
			break
		}
		d.sTree.removeChild()
	}
	return found
}

type SearchNode struct {
	H        Graph
	extVerts []int
	sepGen   *SeparatorIt
	sep      lib.Edges
	bag      []int
	myComps  []Graph

	parent   *SearchNode
	children []*SearchNode
}

type SearchTree struct {
	root *SearchNode
	curr *SearchNode
}

func (tree *SearchTree) makeChild(H Graph, sepGen *SeparatorIt) *SearchNode {
	n := &SearchNode{H: H, sepGen: sepGen}
	n.parent = tree.curr
	if tree.root == nil {
		tree.root = n
	} else {
		tree.curr.children = append(tree.curr.children, n)
	}
	tree.curr = n
	return n
}

func (tree *SearchTree) removeChild() {
	n := tree.curr
	tree.curr = n.parent
	if n.parent == nil {
		tree.root = nil
	} else {
		ch := tree.curr.children
		tree.curr.children = tree.curr.children[:len(ch)-1]
	}
	// TODO clean n, no memory waste
}

func (tree *SearchTree) moveUp() {
	tree.curr = tree.curr.parent
}

func (tree *SearchTree) dfs() []*SearchNode {
	var res []*SearchNode
	var n *SearchNode
	var open []*SearchNode
	if tree.root != nil {
		open = append(open, tree.root)
	}
	for len(open) > 0 {
		n, open = open[len(open)-1], open[:len(open)-1]
		res = append(res, n)
		for i := range n.children {
			open = append(open, n.children[len(n.children)-i-1])
		}
	}
	return res
}

func makeSepGen(hg Graph, k int, edges lib.Edges, oldSep []int) *SeparatorIt {
	verticesCurrent := hg.Vertices()
	conn := lib.Inter(oldSep, verticesCurrent)
	compVertices := lib.Diff(verticesCurrent, oldSep)
	bound := lib.FilterVertices(edges, conn)
	sepIt := &SeparatorIt{
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

type SeparatorIt struct {
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

func (s *SeparatorIt) update() {
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

func (s *SeparatorIt) HasNext() bool {
	nextEmpty := reflect.DeepEqual(s.next, lib.Edges{})
	if (nextEmpty && !s.delivered) || (!nextEmpty && s.delivered) {
		s.update()
	}
	nextEmpty = reflect.DeepEqual(s.next, lib.Edges{})
	return !nextEmpty && !s.delivered
}

func (s *SeparatorIt) Next() lib.Edges {
	if !s.HasNext() {
		panic(fmt.Errorf("wrong state"))
	}
	s.delivered = true
	return s.next
}
