package decomp

import (
	"fmt"
	"sort"

	"github.com/cem-okulmus/BalancedGo/lib"
)

type Decomp = lib.Decomp
type Graph = lib.Graph

type SearchNode struct {
	hg       Graph
	extVerts []int
	sepGen   *DetKSeparatorIt
	sep      lib.Edges
	bag      []int
	myComps  []Graph

	size int

	parent   *SearchNode
	children []*SearchNode
}

type SearchTree struct {
	root *SearchNode
	curr *SearchNode
}

func (tree *SearchTree) MakeChild(hg Graph, sepGen *DetKSeparatorIt) *SearchNode {
	n := &SearchNode{hg: hg, sepGen: sepGen, size: -1}
	n.parent = tree.curr
	if tree.root == nil {
		tree.root = n
	} else {
		tree.curr.children = append(tree.curr.children, n)
	}
	tree.curr = n
	return n
}

func (tree *SearchTree) RemoveChildren() {
	tree.curr = tree.curr.parent
	if tree.curr == nil {
		tree.root = nil
	} else {
		tree.curr.children = nil
	}
	// TODO clean n, no memory waste
}

func (tree *SearchTree) MoveToParent() {
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

const (
	ShrinkSoftly = "soft"
	ShrinkHardly = "hard"
)

func (tree *SearchTree) Shrink(mode string) {
	hg := tree.root.hg
	nodes := tree.dfs()
	for _, n := range nodes {
		shrinkUp(n, mode)
	}
	nodes = tree.dfs()
	for i := len(nodes) - 1; i >= 0; i-- {
		shrinkDown(tree, nodes[i], mode)
	}
	tree.root.hg = hg
}

func shrinkUp(n *SearchNode, mode string) {
	if n.parent != nil {
		if simplify(n, n.parent, mode) {
			//n.parent.hg = n.hg

			var i int
			if i = posOf(n, n.parent.children); i < 0 {
				panic("n not found")
			}
			n.parent.children[i] = n.parent.children[len(n.parent.children)-1]
			n.parent.children[len(n.parent.children)-1] = nil
			n.parent.children = n.parent.children[:len(n.parent.children)-1]

			for _, c := range n.children {
				c.parent = n.parent
			}
			n.parent.children = append(n.parent.children, n.children...)

			n.parent = nil
			n.children = nil
		}
	}
}

func shrinkDown(tree *SearchTree, n *SearchNode, mode string) {
	for i, child := range n.children {
		if simplify(n, child, mode) {
			//child.hg = n.hg

			n.children[i] = n.children[len(n.children)-1]
			n.children[len(n.children)-1] = nil
			n.children = n.children[:len(n.children)-1]

			for _, c := range n.children {
				c.parent = child
			}
			child.children = append(child.children, n.children...)
			child.parent = n.parent
			if n.parent != nil {
				var i int
				if i = posOf(n, n.parent.children); i < 0 {
					panic("n not found")
				}
				n.parent.children[i] = n.parent.children[len(n.parent.children)-1]
				n.parent.children[len(n.parent.children)-1] = nil
				n.parent.children = n.parent.children[:len(n.parent.children)-1]

				n.parent.children = append(n.parent.children, child)
			} else {
				tree.root = child
			}
			n.parent = nil
			n.children = nil
			break
		}
	}
}

func simplify(n1 *SearchNode, n2 *SearchNode, mode string) bool {
	bagSub := lib.Subset(n1.bag, n2.bag)
	coverSub := lib.Subset(toNameSlice(n1.sep), toNameSlice(n2.sep))
	switch {
	case bagSub && coverSub:
		// eliminate n1
		return true
	case bagSub && !coverSub:
		// join/semijoin case
		if mode == ShrinkHardly {
			// TODO if a sibling contains n1.sep, then you don't need to add
			n2.sep = lib.NewEdges(append(n2.sep.Slice(), n1.sep.Slice()...))
			n2.sep.RemoveDuplicates()
		}
		return mode == ShrinkHardly
	case !bagSub && coverSub:
		// eliminate n1, expand bag
		n2.bag = append(n2.bag, n1.bag...)
		sort.Ints(n2.bag)
		j := 0
		for i := 1; i < len(n2.bag); i++ {
			if n2.bag[j] == n2.bag[i] {
				continue
			}
			j++
			n2.bag[j] = n2.bag[i]
		}
		n2.bag = n2.bag[:j+1]
		return true
	case !bagSub && !coverSub:
		return false
	default:
		panic(fmt.Errorf("impossible"))
	}
}

func posOf(n *SearchNode, nodes []*SearchNode) int {
	for j, m := range nodes {
		if m == n {
			return j
		}
	}
	return -1
}

func toNameSlice(edges lib.Edges) []int {
	var res []int
	for _, e := range edges.Slice() {
		res = append(res, e.Name)
	}
	return res
}

func (tree *SearchTree) Clone() *SearchTree {
	res := &SearchTree{}
	copyTree(res, *tree.root)
	return res
}

func copyTree(tree *SearchTree, n SearchNode) {
	curr := tree.MakeChild(n.hg, nil)
	curr.bag = n.bag
	curr.sep = n.sep
	for _, c := range n.children {
		copyTree(tree, *c)
	}
	tree.MoveToParent()
}

func MakeSearchTree(dec Decomp) *SearchTree {
	tree := &SearchTree{}
	copyDecomp(tree, &dec.Root)
	tree.root.hg = dec.Graph
	return tree
}

func copyDecomp(tree *SearchTree, n *lib.Node) {
	curr := tree.MakeChild(Graph{}, nil)
	curr.bag = n.Bag
	curr.sep = n.Cover
	for _, c := range n.Children {
		copyDecomp(tree, &c)
	}
	tree.MoveToParent()
}

func MakeDecomp(tree SearchTree) Decomp {
	return Decomp{Graph: tree.root.hg, Root: makeDecomp(tree.root)}
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
