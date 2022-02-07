package decomp

import (
	"fmt"

	"github.com/cem-okulmus/BalancedGo/lib"
)

type SubProblem struct {
	Node *SearchNode
	Cost int
}

type PriorityQueue []*SubProblem

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Cost < pq[j].Cost
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*SubProblem)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

type BnbSearchTree struct {
	root *SearchNode
	curr *SearchNode
}

type BnbDetKStreamer struct {
	K     int
	Graph lib.Graph
	sTree BnbSearchTree

	cache lib.Cache

	Ev            *Evaluator
	currOptDecomp Decomp
	currOptCost   int
}

func (d *BnbDetKStreamer) Name() string {
	return "BnbDetK"
}

func (d *BnbDetKStreamer) Stream(stop <-chan bool) <-chan Decomp {
	out := make(chan Decomp)
	go func() {
		defer close(out)

		d.currOptDecomp = Decomp{Graph: d.Graph, Root: lib.Node{Bag: d.Graph.Vertices(), Cover: d.Graph.Edges}}
		d.currOptCost = d.Ev.Eval(d.currOptDecomp)
		select {
		case out <- d.currOptDecomp:
		case <-stop:
			return
		}

		d.cache.Init()
		if found, cost := d.decompose(d.Graph, []int{}); found {
			d.currOptDecomp = MakeDecomp(d.sTree)
			d.currOptCost = cost
			select {
			case out <- d.currOptDecomp:
			case <-stop:
				return
			}
		}
		/*for d.advance() {
			select {
			case out <- d.buildDecomp():
			case <-stop:
				return
			}
		}*/
	}()
	return out
}

func (d *BnbDetKStreamer) decompose(H Graph, oldSep []int) (bool, int) {
	sepGen := NewDetKSepGen(H, d.K, d.Graph.Edges, oldSep)
	n := d.sTree.MakeChild(H, sepGen)
	n.extVerts = append(H.Vertices(), oldSep...)
	found := false
	myCurrCost := 0
	for n.sepGen.HasNext() {
		n.sep = n.sepGen.Next()
		n.bag = lib.Inter(n.sep.Vertices(), n.extVerts)
		myCurrCost = d.Ev.EvalNode(n)
		if myCurrCost > d.currOptCost {
			myCurrCost = 0
			continue
		}
		n.myComps, _, _ = H.GetComponents(n.sep)
		if len(n.myComps) == 0 {
			found = true
			break
		}
		allSubDecomp := true
		for _, Hc := range n.myComps {
			subDecomp, subCost := d.decompose(Hc, n.bag)
			edgeCost := d.Ev.EvalEdge(n, d.sTree.curr)
			myCurrCost += subCost + edgeCost
			if !subDecomp || myCurrCost > d.currOptCost {
				allSubDecomp = false
				myCurrCost = 0
				break
			}
		}
		if allSubDecomp {
			found = true
			break
		}
	}
	if found {
		d.sTree.MoveToParent()
	} else {
		d.sTree.RemoveChildren()
	}
	if d.Ev.EvalTree(&d.sTree) != myCurrCost {
		panic(fmt.Errorf("actual cost != current cost, %v != %v", d.Ev.EvalTree(&d.sTree), myCurrCost))
	}
	return found, myCurrCost
}

/*func (d *BnbDetKStreamer) advance() bool {
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
			for par != nil {
				for i := len(par.children); i < len(par.myComps); i++ {
					Hc := par.myComps[i]
					if !d.decompose(Hc, par.bag) {
						panic(fmt.Errorf("one decomposition should exist"))
					}
				}
				d.sTree.moveUp()
				par = d.sTree.curr
			}
			break
		}
		d.sTree.removeChild()
	}
	return found
}*/
