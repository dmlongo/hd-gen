package decomp

import (
	"fmt"

	"github.com/cem-okulmus/BalancedGo/lib"
)

type Streamer interface {
	Name() string
	Stream(stop <-chan bool) <-chan Decomp
}

type DetKStreamer struct {
	K     int
	Graph lib.Graph
	sTree SearchTree

	cache lib.Cache
}

func (d *DetKStreamer) Name() string {
	return "EnumDetK"
}

func (d *DetKStreamer) Stream(stop <-chan bool) <-chan Decomp {
	out := make(chan Decomp)
	go func() {
		defer close(out)

		d.cache.Init()
		if d.decompose(d.Graph, []int{}) {
			select {
			case out <- MakeDecomp(d.sTree):
			case <-stop:
				return
			}
		}
		for d.advance() {
			select {
			case out <- MakeDecomp(d.sTree):
			case <-stop:
				return
			}
		}
	}()
	return out
}

func (d *DetKStreamer) decompose(H Graph, oldSep []int) bool {
	sepGen := NewDetKSepGen(H, d.K, d.Graph.Edges, oldSep)
	n := d.sTree.MakeChild(H, sepGen)
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
		if d.cache.CheckNegative(n.sep, n.myComps) {
			break
		}
		allSubDecomp := true
		for _, Hc := range n.myComps {
			allSubDecomp = d.decompose(Hc, n.bag)
			if !allSubDecomp {
				d.cache.AddNegative(n.sep, Hc)
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
			n.myComps, _, _ = n.hg.GetComponents(n.sep)
			if len(n.myComps) == 0 {
				found = true
				break
			}
			if d.cache.CheckNegative(n.sep, n.myComps) {
				break
			}
			allSubDecomp := true
			for _, Hc := range n.myComps {
				allSubDecomp = d.decompose(Hc, n.bag)
				if !allSubDecomp {
					d.cache.AddNegative(n.sep, Hc)
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
			par := d.sTree.curr
			for par != nil {
				for i := len(par.children); i < len(par.myComps); i++ {
					Hc := par.myComps[i]
					if !d.decompose(Hc, par.bag) {
						panic(fmt.Errorf("one decomposition should exist"))
					}
				}
				d.sTree.MoveToParent()
				par = d.sTree.curr
			}
			break
		}
		d.sTree.RemoveChildren()
	}
	return found
}

type BestDetKStreamer struct {
	DetK *DetKStreamer
	Ev   *Evaluator
}

func (b *BestDetKStreamer) Name() string {
	return "BestDetK"
}

func (b *BestDetKStreamer) Stream(stop <-chan bool) <-chan Decomp {
	out := make(chan Decomp)
	go func() {
		defer close(out)

		var currDecomp Decomp
		currCost := int(^uint(0) >> 1) // max int
		for dec := range b.DetK.Stream(stop) {
			cost := b.Ev.Eval(dec)
			if cost < currCost {
				currDecomp = dec
				currCost = cost
			}
		}
		// todo what about the stop channel?
		out <- currDecomp
	}()
	return out
}

type BnbDetKStreamer struct {
	K     int
	Graph lib.Graph
	sTree SearchTree

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
