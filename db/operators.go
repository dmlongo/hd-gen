package db

type Condition func(t Tuple) bool

func Semijoin(l *Table, r Table) (*Table, bool) {
	joinIdx := commonAttrs(*l, r)
	if len(joinIdx) == 0 {
		return l, false
	}

	var tupToDel []int
	for i, leftTup := range l.Tuples {
		delete := true
		for _, rightTup := range r.Tuples {
			if match(leftTup, rightTup, joinIdx) {
				delete = false
				break
			}
		}
		if delete {
			tupToDel = append(tupToDel, i)
		}
	}

	res, err := l.RemoveTuples(tupToDel)
	if err != nil {
		panic(err)
	}
	return l, res
}

func Join(l Table, r Table) *Table {
	if l.Size() < r.Size() {
		l, r = r, l
	}
	joinIdx := commonAttrs(l, r)
	newAttrs := joinedAttrs(l, r)
	newTab := NewTable(newAttrs, false) // todo compute stats?
	for _, lTup := range l.Tuples {
		for _, rTup := range r.Tuples {
			if match(lTup, rTup, joinIdx) {
				newTup := joinedTuple(newAttrs, lTup, rTup, r.attrPos)
				newTab.AddTuple(newTup)
			}
		}
	}
	return newTab
}

func Select(r *Table, c Condition) (*Table, bool) {
	var tupToDel []int
	for i, tup := range r.Tuples {
		if !c(tup) {
			tupToDel = append(tupToDel, i)
		}
	}
	res, err := r.RemoveTuples(tupToDel)
	if err != nil {
		panic(err)
	}
	return r, res
}

func commonAttrs(left Table, right Table) [][]int {
	var out [][]int
	rev := len(right.attrs) < len(left.attrs)
	if rev {
		left, right = right, left
	}
	for iLeft, varLeft := range left.attrs {
		if iRight, found := right.attrPos[varLeft]; found {
			if rev {
				out = append(out, []int{iRight, iLeft})
			} else {
				out = append(out, []int{iLeft, iRight})
			}
		}
	}
	return out
}

func match(left Tuple, right Tuple, joinIndex [][]int) bool {
	for _, z := range joinIndex {
		if left[z[0]] != right[z[1]] {
			return false
		}
	}
	return true
}

func joinedAttrs(l Table, r Table) []string {
	var res []string
	res = append(res, l.attrs...)
	for _, v := range r.attrs {
		if _, found := l.attrPos[v]; !found {
			res = append(res, v)
		}
	}
	return res
}

func joinedTuple(attrs []string, lTup Tuple, rTup Tuple, rAttrPos map[string]int) Tuple {
	res := make(Tuple, 0, len(attrs))
	res = append(res, lTup...)
	for _, v := range attrs[len(lTup):] {
		i := rAttrPos[v]
		res = append(res, rTup[i])
	}
	return res
}
