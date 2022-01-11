package decomp

import (
	"github.com/dmlongo/hd-gen/db"
)

type Yannakakis interface {
	// Solve the problem represented by the given tree
	BoolAnswer() bool

	// AllSolutions of the problem represent by the given tree
	AllAnswers() *db.Table

	// join tables in every node
	computeNodes(curr *yNode) bool
	// reduce a tree with upwards semijoins
	reduce(root *yNode) bool
	// fullyReduce a tree with downwards semijoins (after reduce)
	fullyReduce(root *yNode)
	// joinUpwards a tree to compute all solutions (after fullyReduce)
	joinUpwards(root *yNode) *db.Table
}

func MakeYannakakis(tree *SearchTree, e2t map[int]string, d db.Database) Yannakakis {
	var yRoot *yNode
	var n *SearchNode
	var p *yNode
	var open []*SearchNode
	var parents []*yNode
	if tree.root != nil {
		open = append(open, tree.root)
		parents = append(parents, nil)
	}
	for len(open) > 0 {
		n, open = open[len(open)-1], open[:len(open)-1]
		p, parents = parents[len(parents)-1], parents[:len(parents)-1]

		y := &yNode{}
		for _, e := range n.sep.Slice() {
			t := d[e2t[e.Name]]
			y.tables = append(y.tables, t)
		}
		// todo proj = bag missing
		if p != nil {
			p.children = append(p.children, y)
		}
		if yRoot == nil {
			yRoot = y
		}

		for i := range n.children {
			open = append(open, n.children[len(n.children)-i-1])
			parents = append(parents, y)
		}
	}
	return &yTree{root: yRoot}
}

type yNode struct {
	tables []*db.Table

	join *db.Table
	//proj []string

	children []*yNode
}

type yTree struct {
	root *yNode
}

func (y *yTree) BoolAnswer() bool {
	return y.computeNodes(y.root) && y.reduce(y.root)
}

func (y *yTree) AllAnswers() *db.Table {
	if y.computeNodes(y.root) && y.reduce(y.root) {
		y.fullyReduce(y.root)
		return y.joinUpwards(y.root)
	}
	return nil
}

func (y *yTree) computeNodes(curr *yNode) bool {
	if !y.joinNode(curr) {
		return false
	}
	for _, child := range curr.children {
		if !y.computeNodes(child) {
			return false
		}
	}
	return true
}

func (y *yTree) joinNode(curr *yNode) bool {
	if curr.join == nil {
		if len(curr.tables) == 1 {
			curr.join = curr.tables[0]
		} else {
			curr.join = db.Join(*curr.tables[0], *curr.tables[1])
			for i := 2; i < len(curr.tables) && !curr.join.Empty(); i++ {
				curr.join = db.Join(*curr.join, *curr.tables[i])
			}
		}
	}
	return !curr.join.Empty()
}

// bottom-up phase
func (y *yTree) reduce(curr *yNode) bool {
	for _, child := range curr.children {
		if !y.reduce(child) {
			return false
		}
		db.Semijoin(curr.join, *child.join)
		if curr.join.Empty() {
			return false
		}
	}
	return true
}

// top-down phase
func (y *yTree) fullyReduce(root *yNode) {
	for _, child := range root.children {
		db.Semijoin(child.join, *root.join)
		y.fullyReduce(child)
	}
}

func (y *yTree) joinUpwards(curr *yNode) *db.Table {
	for _, child := range curr.children {
		child.join = y.joinUpwards(child)
		curr.join = db.Join(*curr.join, *child.join)
	}
	return curr.join
}
