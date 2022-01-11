package db

func TablesEqual(t1 Table, t2 Table) bool {
	if len(t1.attrs) != len(t2.attrs) {
		return false
	}
	for i, attr := range t1.attrs {
		if t2.attrs[i] != attr {
			return false
		}
	}
	if t1.Size() != t2.Size() {
		return false
	}
	for i := range t1.Tuples {
		if !TuplesEqual(t1.Tuples[i], t2.Tuples[i]) {
			return false
		}
	}
	return true
}

func TuplesEqual(t1 Tuple, t2 Tuple) bool {
	if len(t1) != len(t2) {
		return false
	}
	for i := range t1 {
		if t1[i] != t2[i] {
			return false
		}
	}
	return true
}

func TablesDeepEqual(tab1 Table, tab2 Table) bool {
	if len(tab1.attrs) != len(tab2.attrs) {
		return false
	}
	for _, attr := range tab1.Attributes() {
		if _, ok := tab2.Position(attr); !ok {
			return false
		}
	}
	for _, attr := range tab2.Attributes() {
		if _, ok := tab1.Position(attr); !ok {
			return false
		}
	}
	if tab1.Size() != tab2.Size() {
		return false
	}
	for _, tup1 := range tab1.Tuples {
		if !containsTuple(tup1, tab1.attrs, &tab2) {
			return false
		}
	}
	for _, tup2 := range tab2.Tuples {
		if !containsTuple(tup2, tab2.attrs, &tab1) {
			return false
		}
	}
	return true
}

func containsTuple(tup Tuple, attrs []string, tab *Table) bool {
	for _, tup2 := range tab.Tuples {
		found := true
		if len(tup) != len(tup2) {
			found = false
			break
		}
		for i := 0; i < len(tup); i++ {
			p2, _ := tab.Position(attrs[i])
			if tup[i] != tup2[p2] {
				found = false
				break
			}
		}
		if found {
			return true
		}
	}
	return false
}
