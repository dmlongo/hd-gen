package main

import (
	"fmt"
	"sort"

	"github.com/cem-okulmus/BalancedGo/lib"
)

const (
	soft = "soft"
	hard = "hard"
)

func Shrink(decomp Decomp, mode string) Decomp {
	sTree := makeSearchTree(&decomp.Root)
	nodes := sTree.dfs()
	for _, n := range nodes {
		shrinkUp(n, mode)
	}
	nodes = sTree.dfs()
	for i := len(nodes) - 1; i >= 0; i-- {
		shrinkDown(sTree, nodes[i], mode)
	}
	return Decomp{Graph: decomp.Graph, Root: makeDecomp(sTree.root)}
}

func shrinkUp(n *SearchNode, mode string) {
	if n.parent != nil {
		if simplify(n, n.parent, mode) {
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
		if mode == hard {
			// TODO if a sibling contains n1.sep, then you don't need to add
			n2.sep = lib.NewEdges(append(n2.sep.Slice(), n1.sep.Slice()...))
			n2.sep.RemoveDuplicates()
		}
		return mode == hard
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

func makeSearchTree(dRoot *lib.Node) *SearchTree {
	tree := &SearchTree{}
	copyTree(tree, dRoot)
	return tree
}

func copyTree(tree *SearchTree, n *lib.Node) {
	curr := tree.makeChild(Graph{}, nil)
	curr.bag = n.Bag
	curr.sep = n.Cover
	for _, c := range n.Children {
		copyTree(tree, &c)
	}
	tree.moveUp()
}

func toNameSlice(edges lib.Edges) []int {
	var res []int
	for _, e := range edges.Slice() {
		res = append(res, e.Name)
	}
	return res
}
