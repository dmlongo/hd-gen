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

func (qe Evaluator) GreedyJoinPlan(n *SearchNode) ([]lib.Edge, []*db.Statistics, []int, int) {
	sep := n.sep.Slice()
	var tables []*db.Statistics
	for _, e := range sep {
		t, _ := qe.StatsDB.Stats(lib.NewEdges([]lib.Edge{e}))
		tables = append(tables, t)
	}

	if len(tables) <= 2 {
		cost, _ := db.EstimateJoinSize(tables)
		return sep, tables, []int{0, 1}[:len(tables)], cost
	}

	var eOrder []lib.Edge
	var jOrder []*db.Statistics
	var indices []int
	cost := 0

	choices := make(map[int]*db.Statistics)
	for i := range tables {
		choices[i] = tables[i]
	}

	idx1, idx2 := -1, -1
	min := int(^uint(0) >> 1)
	for i, t1 := range tables {
		for j := i + 1; j < len(tables); j++ {
			t2 := tables[j]
			s, _ := db.EstimateJoinSize([]*db.Statistics{t1, t2})
			if s < min {
				min = s
				idx1, idx2 = i, j
			}
		}
	}
	eOrder = append(eOrder, sep[idx1], sep[idx2])
	jOrder = append(jOrder, tables[idx1], tables[idx2])
	indices = append(indices, idx1, idx2)
	delete(choices, idx1)
	delete(choices, idx2)
	cost += min

	for len(choices) > 1 {
		min = int(^uint(0) >> 1)
		idx := -1
		tmp := jOrder
		for i, t := range choices {
			tmp = append(tmp, t)
			s, _ := db.EstimateJoinSize(tmp)
			if s < min {
				min = s
				idx = i
			}
			tmp = tmp[:len(tmp)-1]
		}
		eOrder = append(eOrder, sep[idx])
		jOrder = append(jOrder, tables[idx])
		indices = append(indices, idx)
		delete(choices, idx)
		cost += min
	}
	for i, t := range choices {
		eOrder = append(eOrder, sep[i])
		jOrder = append(jOrder, t)
		indices = append(indices, i)
	}

	return eOrder, jOrder, indices, cost
}
