package decomp

import (
	"math"

	"github.com/cem-okulmus/BalancedGo/lib"
	"github.com/dmlongo/hd-gen/db"
)

type Evaluator interface {
	Eval(dec Decomp) int
	EvalTree(tree *SearchTree) int
	EvalNode(n *SearchNode) int
	EvalEdge(par *SearchNode, child *SearchNode) int
}

type InformedEvaluator struct {
	Db  db.Database
	Edge2Table map[int]string // edge -> table
}

func (qe InformedEvaluator) Eval(dec Decomp) int {
	tree := MakeSearchTree(dec)
	return qe.EvalTree(tree)
}

func (qe InformedEvaluator) EvalTree(tree *SearchTree) int {
	cost := 0
	var n *SearchNode
	dfs := tree.dfs()
	for len(dfs) > 0 {
		n, dfs = dfs[len(dfs)-1], dfs[:len(dfs)-1]
		cost += qe.EvalNode(n)
		for _, child := range n.children {
			cost += qe.EvalEdge(n, child)
		}
	}
	return cost
}

func (qe InformedEvaluator) EvalNode(n *SearchNode) int {
	if n.size == -1 {
		jTables := qe.toTables(n.sep)
		n.size = db.HgramJoinSize(jTables)
	}
	return n.size
}

func (qe InformedEvaluator) EvalEdge(par *SearchNode, child *SearchNode) int {
	if par.size == -1 || child.size == -1 {
		panic("negative sizes")
	}
	if par.size == 0 || child.size == 0 {
		return 0
	}

	parTables := qe.toTables(par.sep)
	childTables := qe.toTables(child.sep)
	if len(par.sep.Slice()) == 1 && len(child.sep.Slice()) == 1 {
		return db.HgramSemijoinSize(parTables[0], childTables[0])
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

func (qe InformedEvaluator) toTables(edges lib.Edges) []db.Table {
	var tables []db.Table
	for _, e := range edges.Slice() {
		tabName := qe.Edge2Table[e.Name]
		tables = append(tables, *qe.Db[tabName]) // todo pay attention to this
	}
	return tables
}

type EstimateEvaluator struct {
	Sizes SizeEstimates
}

func (uqe EstimateEvaluator) Eval(dec Decomp) int {
	tree := MakeSearchTree(dec)
	return uqe.EvalTree(tree)
}

func (uqe EstimateEvaluator) EvalTree(tree *SearchTree) int {
	cost := 0
	var n *SearchNode
	dfs := tree.dfs()
	for len(dfs) > 0 {
		n, dfs = dfs[len(dfs)-1], dfs[:len(dfs)-1]
		cost += uqe.EvalNode(n)
		for _, child := range n.children {
			cost += uqe.EvalEdge(n, child)
		}
	}
	return cost
}

func (uqe EstimateEvaluator) EvalNode(n *SearchNode) int {
	if n.size == -1 {
		n.size = uqe.Sizes.Cost(n.sep)
	}
	return n.size
}

func (uqe EstimateEvaluator) EvalEdge(par *SearchNode, child *SearchNode) int {
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
		den *= uqe.Sizes.Cost(lib.NewEdges([]lib.Edge{t}))
	}
	res := int(math.Round(float64(num) / float64(den)))
	par.size = res
	return res
}
