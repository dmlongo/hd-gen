package decomp

import (
	"fmt"

	"github.com/cem-okulmus/BalancedGo/lib"
	"github.com/dmlongo/hd-gen/db"
)

type Evaluator struct {
	StatsDB StatisticsDB
}

func (qe Evaluator) Eval(dec Decomp) int {
	tree := MakeSearchTree(dec)
	return qe.EvalTree(tree)
}

func (qe Evaluator) EvalTree(tree *SearchTree) int {
	cost := 0
	var n *SearchNode
	dfs := tree.dfs()
	for len(dfs) > 0 {
		n, dfs = dfs[len(dfs)-1], dfs[:len(dfs)-1]
		cost += qe.EvalNode(n)
		// todo order children by selectivity?
		// actually order should be irrelevant for cost
		for _, child := range n.children {
			cost += qe.EvalEdge(n, child)
		}
	}
	return cost
}

func (qe Evaluator) EvalNode(n *SearchNode) int {
	if _, ok := qe.StatsDB.Stats(n.sep); !ok {
		var jTables []*db.Statistics
		for _, e := range n.sep.Slice() {
			edges := lib.NewEdges([]lib.Edge{e})
			if eStats, ok := qe.StatsDB.Stats(edges); !ok {
				panic(fmt.Errorf("no stats for single edge %v", e.Name))
			} else {
				jTables = append(jTables, eStats)
			}
		}
		_, stats := db.EstimateJoinSize(jTables)
		qe.StatsDB.Put(n.sep, stats)
	}

	stats, _ := qe.StatsDB.Stats(n.sep)
	return stats.Size
}

func (qe Evaluator) EvalEdge(par *SearchNode, child *SearchNode) int {
	parStats, parOk := qe.StatsDB.Stats(par.sep)
	childStats, childOk := qe.StatsDB.Stats(child.sep)

	if !parOk || !childOk {
		panic(fmt.Errorf("no stats for edge (%v,%v)", par.sep, child.sep))
	}

	newParSize, newParStats := db.EstimateSemijoinSize(parStats, childStats)
	qe.StatsDB.Put(par.sep, newParStats)
	return newParSize

	/*parTables := qe.toTables(par.sep)
	childTables := qe.toTables(child.sep)
	if len(par.sep.Slice()) == 1 && len(child.sep.Slice()) == 1 {
		return db.HgramSemijoinSize(parTables[0], childTables[0])
	} // non funziona, non tengo conto che par puo' avere altri figli*/

	// size_{child} = sel_{child} * par.size
	// sel_{child} = (expected cardinality of q_{child}) / (prod of q_{child} tables)
	/*num := child.size * par.size
	den := 1
	childTables := qe.toTables(child.sep)
	for _, t := range childTables {
		den *= t.Size()
	}
	return int(math.Round(float64(num) / float64(den)))
	// I think there are smarter ways to do this
	*/
}
